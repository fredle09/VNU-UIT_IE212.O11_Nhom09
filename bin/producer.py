import csv
from kafka import KafkaProducer

from bin.config import *


class Producer:
    """
    A class that represents a producer for streaming data to Kafka.

    Attributes:
        topic (str): The name of the Kafka topic to stream the data to.
        csv_file_path (str optional): The path to the CSV file containing the data to be streamed.
        schema_list (list[str]): A list of strings representing the schema of the data.
        spark (SparkSession, optional): The SparkSession object for reading the CSV file.
        schema_data_types (dict[str, Any]): A dictionary mapping column names to their corresponding data types.

    Methods:
        store_csv_to_kafka(): Reads the CSV file and stores the data to Kafka.
        store_dataframe_to_kafka(df: DataFrame): Stores a DataFrame to Kafka.
        streaming_data_to_kafka(): Streams the data from the CSV file to Kafka in real-time.
    """

    def __init__(
        self,
        topic: str,
        schema_list: list[str],
        csv_file_path: Optional[str] = None,
        spark_session: Optional[SparkSession] = None,
    ) -> None:
        """
        Initializes a Producer object.

        Args:
            topic (str): The name of the Kafka topic to stream the data to.
            schema_list (list[str]): A list of strings representing the schema of the data.
            csv_file_path (Optional[str]): The path to the CSV file containing the data (default: None).
            spark_session (Optional[SparkSession]): The SparkSession object for processing the data (default: None).

        Returns:
            None
        """
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json_dumps(x).encode("utf-8"),
        )
        self.topic: str = topic
        self.schema_list: list[str] = schema_list
        self.csv_file_path: Optional[str] = csv_file_path
        self.spark_session: Optional[SparkSession] = spark_session
        self.schema_data_types: dict[str, Any] = {}
        for col_info in self.schema_list:
            col_name, col_type = col_info.split(" ")
            if col_type == "INT":
                self.schema_data_types[col_name] = int
            elif col_type == "DOUBLE":
                self.schema_data_types[col_name] = float
            elif col_type == "TIMESTAMP":
                self.schema_data_types[col_name] = datetime
            else:
                self.schema_data_types[col_name] = str

    def store_csv_to_kafka(self) -> None:
        """
        Reads a CSV file using the Spark session and stores the data to Kafka.

        Raises:
            ValueError: If the CSV file path is invalid or if the Spark session is invalid.
        """
        if not self.csv_file_path:
            raise ValueError("Invalid path to csv file")
        if not os.path.exists(self.csv_file_path):
            raise ValueError(f"Invalid path to csv file at {self.csv_file_path}")
        if not self.spark_session:
            raise ValueError("Invalid spark session")

        df: DataFrame = self.spark_session.read.csv(
            path=self.csv_file_path,
            schema=", ".join(self.schema_list),
        )

        return self.store_dataframe_to_kafka(df)

    def store_dataframe_to_kafka(self, df: DataFrame) -> None:
        """
        Stores a DataFrame to Kafka.

        Args:
            df (DataFrame): The DataFrame to be stored.

        Returns:
            None
        """
        sql_exprs = ["to_json(struct(*)) AS value"]

        # Apply the date_format function to each timestamp column
        df = df.withColumns(
            {
                col_name: F.date_format(col_name, "yyyy-MM-dd HH:mm:ss")
                # Timestamp columns are in the format "yyyy-MM-dd HH:mm:ss"
                for col_name in [
                    col[0] for col in df.dtypes if "timestamp" in col[1].lower()
                ]
            }
        )

        return (
            df.selectExpr(*sql_exprs)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", self.topic)
            .save()
        )

    def __convert_str_to_datetime(
        self, datetime_str: Optional[str]
    ) -> Optional[datetime]:
        """
        Convert a string representation of datetime to a datetime object.

        Args:
            datetime_str (str): The string representation of datetime in the format "%Y-%m-%d %H:%M".

        Returns:
            datetime: The datetime object converted from the string, or None if the input is empty.

        """
        if not datetime_str:
            return None
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    def __get_time_diff(self, datetime_1: datetime, datetime_2: datetime) -> float:
        """
        Calculate the time difference in minutes between two datetime objects.

        Args:
            datetime_1 (datetime): The first datetime object.
            datetime_2 (datetime): The second datetime object.

        Returns:
            float: The time difference in 15 minutes.
        """
        return (datetime_1 - datetime_2).total_seconds() / 15 / 60

    def __convert_date_types_of_record(self, record: dict) -> dict:
        """
        Convert the date types of the record according to the schema data types.

        Args:
            record (dict): The record to be converted.

        Returns:
            dict: The converted record.
        """
        for col_name, col_type in self.schema_data_types.items():
            if col_type != datetime:
                record[col_name] = col_type(record[col_name])
            else:
                record[col_name] = self.__convert_str_to_datetime(record[col_name])

        return record

    def __send_to_kafka(self, record: dict) -> None:
        """
        Sends a record to Kafka.

        Args:
            record (dict): The record to be sent to Kafka.

        Returns:
            None
        """
        record["Timestamp"] = record["Timestamp"].strftime("%Y-%m-%d %H:%M")
        self.producer.send(self.topic, value=record)

    def streaming_data_to_kafka(self) -> None:
        """
        Stream data from a CSV file to Kafka.

        Raises:
            ValueError: If the path to the CSV file is invalid.
        """
        if not self.csv_file_path:
            raise ValueError("Invalid path to csv file")
        if not os.path.exists(self.csv_file_path):
            raise ValueError(f"Invalid path to csv file at {self.csv_file_path}")

        with open(self.csv_file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=list(self.schema_data_types.keys()))

            row: dict = next(reader)
            row = self.__convert_date_types_of_record(row)
            previous_time: Optional[datetime] = None
            current_time: datetime = row["Timestamp"]

            # Ensure that the first data is streamed at the start time
            if previous_time and current_time > previous_time:
                time_diff: float = self.__get_time_diff(current_time, previous_time)
                sleep(time_diff * DELAY)

            # Streaming data from the current row
            while True:
                # Set time point to calculate the time to sleep
                time_point: datetime = datetime.now()

                # Handle the case where the next data has the same time as the
                # current data
                while True:
                    self.__send_to_kafka(row)

                    # Handle the case where the streaming all the data
                    try:
                        row = next(reader)
                        row = self.__convert_date_types_of_record(row)

                    except StopIteration:
                        print("Reached end of file")
                        return

                    # Break if the next data has the different time
                    if row["Timestamp"] > current_time:
                        break

                # Update the previous time and current time
                previous_time = current_time
                current_time = row["Timestamp"]

                # Sleep to simulate the real-time data stream
                diff_time: float = self.__get_time_diff(current_time, previous_time)
                time_process: float = (datetime.now() - time_point).total_seconds()
                sleep(diff_time * DELAY - time_process)


def using_producer(csv_file_path: str) -> None:
    pro: Producer = Producer(
        topic=CAPTURE_TOPIC,
        csv_file_path=csv_file_path,
        schema_list=CAPTURE_SCHEMA_LIST,
    )
    pro.streaming_data_to_kafka()
