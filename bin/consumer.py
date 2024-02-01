# import os
# import pandas as pd
# from datetime import datetime, timedelta
# from typing import Callable

# from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql.streaming.query import StreamingQuery

# from config import *


# def read_streaming(
#     spark: SparkSession,
#     topic: str,
#     schema_JSON: list[str]
# ) -> DataFrame:
#     """
#     Reads data from a Kafka topic using SparkSession and returns a Streaming DataFrame.

#     Args:
#         spark (SparkSession): The SparkSession object.
#         topic (str): The Kafka topic to read from.

#     Returns:
#         DataFrame: The DataFrame containing the read data.
#     """
#     topic = topic or STORE_TOPIC

#     res: DataFrame = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BROKER)
#         .option("subscribe", topic)
#         .option("startingOffsets", "latest")
#         .load()
#     )

#     res = (
#         res.selectExpr("CAST(value AS STRING)")
#         .select(F.from_json("value", ", ".join(schema_JSON)).alias("data"))
#         .selectExpr("data.*")
#     )

#     return res


# def read_history(
#     spark: SparkSession,
#     topic: str,
#     schema_JSON: list[str],
# ) -> DataFrame:
#     """
#     Reads data from a Kafka topic using SparkSession and returns a DataFrame.

#     Args:
#         spark (SparkSession): The SparkSession object.
#         topic (str): The Kafka topic to read from.

#     Returns:
#         DataFrame: The DataFrame containing the read data.
#     """

#     res: DataFrame = (
#         spark.read.format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BROKER)
#         .option("subscribe", topic)
#         .option("startingOffsets", "earliest")
#         .load()
#     )

#     res = (
#         res.selectExpr("CAST(value AS STRING)")
#         .select(F.from_json("value", ", ".join(schema_JSON)).alias("data"))
#         .selectExpr("data.*")
#     )

#     return res


# def consumer_data_batch_with_handle(
#     spark: SparkSession,
#     func: Callable[[DataFrame, int], None],
#     topic: str,
# ) -> StreamingQuery:
#     """
#     Consume data from a Kafka topic in batch mode and process it using the provided function.

#     Args:
#         spark (SparkSession): The Spark session.
#         func (Callable[[DataFrame, int], None]): The function to process the data batch.
#         topic (str, optional): The Kafka topic to consume from. Defaults to STORE_TOPIC.
#         latest (bool, optional): Whether to consume the latest data or not. Defaults to True.

#     Returns:
#         StreamingQuery: The streaming query object representing the running query.
#     """
#     res: StreamingQuery
#     streaming_df: DataFrame = read_streaming(spark, topic, STORE_SCHEMA_LIST)
#     res = (
#         streaming_df.writeStream.trigger(processingTime=f"{DELAY} seconds")
#         .foreachBatch(func)
#         .start()
#     )

#     return res


# def replace_batch_into_csv(df: DataFrame, epoch_id: int) -> None:
#     """
#     Writes a batch of data into csv file.

#     Args:
#         df (DataFrame): The DataFrame containing the data to write.
#         epoch_id (int): The epoch ID.

#     Returns:
#         None
#     """
#     if not os.path.exists(BATCH_FOLDER):
#         os.makedirs(BATCH_FOLDER)

#     df.toPandas().to_csv(
#         f"{BATCH_FOLDER}batch.csv",
#         header=True,
#         mode="w+",
#         index=False,
#     )


# def append_batch_into_csv(df: DataFrame, epoch_id: int) -> None:
#     """
#     Writes a batch of data into csv file.

#     Args:
#         df (DataFrame): The DataFrame containing the data to write.
#         epoch_id (int): The epoch ID.

#     Returns:
#         None
#     """

#     BATCH_FILE: str = f"{BATCH_FOLDER}cache.csv"

#     def add_datetime_col_and_group_by(df: DataFrame) -> DataFrame:
#         """
#         Add a datetime column to the DataFrame.
#         """
#         res: DataFrame = (
#             df.select("Boro", "Yr", "M", "D", "HH", "MM", "Vol")
#             .groupBy("Boro", "Yr", "M", "D", "HH", "MM")
#             .agg(F.sum(F.col("Vol")).alias("Sum_Vol"), F.count("*").alias("Count"))
#             .withColumn(
#                 "Arg_Vol",
#                 F.when(F.col("Count") == 0, 0).otherwise(
#                     F.col("Sum_Vol") / F.col("Count")
#                 ),
#             )
#             .drop("Sum_Vol", "Count")
#             .withColumn("Date", F.concat_ws("-", F.col("Yr"), F.col("M"), F.col("D")))
#             .withColumn("Time", F.concat_ws(":", F.col("HH"), F.col("MM")))
#             .withColumn("Datetime", F.concat_ws(" ", F.col("Date"), F.col("Time")))
#             .withColumn("Datetime", F.to_timestamp(F.col("Datetime")))
#             .drop("Date", "Time", "Yr", "M", "D", "HH", "MM")
#             .orderBy("Datetime", "Boro")
#         )
#         return res

#     def get_datetime_filter(datetime_obj: datetime, hours: int) -> datetime:
#         """
#         Get a datetime filter by subtracting the specified number of days from the given datetime string.
#         """
#         res: datetime = datetime_obj - timedelta(hours=hours)
#         return res

#     def remove_expire_record_from_csv(
#         pdf: pd.DataFrame,
#         current_datetime_obj: datetime,
#         filter_datetime_obj: datetime,
#     ) -> None:
#         """
#         Remove records that are older than the specified datetime object and save the result to the csv file.
#         """
#         pdf = pdf[
#             (pdf["Datetime"] >= filter_datetime_obj)
#             & (pdf["Datetime"] < current_datetime_obj)
#         ]
#         pdf.to_csv(
#             BATCH_FILE,
#             header=False,
#             mode="w+",
#             index=False,
#         )

#     def append_record_to_csv(df: DataFrame) -> None:
#         """
#         Append a record to the DataFrame.
#         """
#         df.toPandas().to_csv(
#             BATCH_FILE,
#             header=False,
#             mode="a",
#             index=False,
#         )

#     df = add_datetime_col_and_group_by(df)

#     # Create batch_folder if not exists
#     if not os.path.exists(BATCH_FOLDER):
#         os.makedirs(BATCH_FOLDER)

#     # If cache.csv exists, remove expired records
#     if os.path.exists(BATCH_FILE) and df.first() and df.first().Datetime:  # type: ignore
#         filter_datetime_obj: datetime = get_datetime_filter(
#             datetime_obj=df.first().Datetime,  # type: ignore
#             hours=EXPIRE_TIME,
#         )

#         try:
#             pdf: pd.DataFrame = pd.read_csv(BATCH_FILE, header=None)
#             pdf.columns = ["Boro", "Arg_Vol", "Datetime"]
#             pdf.Datetime = pd.to_datetime(pdf.Datetime)
#             remove_expire_record_from_csv(pdf, df.first().Datetime, filter_datetime_obj)  # type: ignore
#         except pd.errors.EmptyDataError:
#             pass

#     # Append the new batch to the csv file
#     append_record_to_csv(df)


# if __name__ == "__main__":
#     spark: SparkSession = (
#         SparkSession.builder.appName("KafkaConsumer")  # type: ignore
#         .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
#         .config("spark.sql.streaming.checkpointLocation", "checkpoint")
#         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
#         .getOrCreate()
#     )

#     streaming_query_batch = consumer_data_batch_with_handle(
#         spark=spark,
#         func=replace_batch_into_csv,
#         topic=STORE_TOPIC,
#     )

#     streaming_query_batch_2 = consumer_data_batch_with_handle(
#         spark=spark,
#         func=append_batch_into_csv,
#         topic=STORE_TOPIC,
#     )

#     try:
#         streaming_query_batch.awaitTermination()
#         streaming_query_batch_2.awaitTermination()
#     except KeyboardInterrupt:
#         print("Stopping streaming query...")
#         streaming_query_batch.stop()  # type: ignore
#         streaming_query_batch_2.stop()  # type: ignore
#         print("Streaming query stopped.")

from config import *


class Consumer:
    """
    A class representing a consumer for streaming data.

    Attributes:
        spark (SparkSession): The Spark session.
        topic (str): The Kafka topic to consume from.
        schema_list (list[str]): A list of schema strings for parsing the data.

    Methods:
        get_streaming_dataframe(): Get the streaming DataFrame from Kafka.
        handle_batch_streaming_with_callable(df: Streaming DataFrame, func: Callable[[DataFrame, int], None]): Handle batch streaming with a callable function.
        get_history_dataframe(Optional[from_timestamp]): Get the history DataFrame from Kafka.
    """

    def __init__(
        self, topic: str, schema_list: list[str], spark_session: SparkSession
    ) -> None:
        self.topic: str = topic
        self.schema_list: list[str] = schema_list
        self.spark: SparkSession = spark_session

    def get_streaming_df(self) -> DataFrame:
        """
        Get the streaming DataFrame from Kafka.

        Returns:
            DataFrame: The streaming DataFrame.
        """
        raw_streaming_df: DataFrame = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

        streaming_df: DataFrame = (
            raw_streaming_df.selectExpr("CAST(value AS STRING)")
            .select(F.from_json("value", ", ".join(self.schema_list)).alias("data"))
            .selectExpr("data.*")
        )

        return streaming_df

    def handle_batch_streaming_with_callable(
        self, df: DataFrame, func: Callable[[DataFrame, int], None]
    ) -> StreamingQuery:
        """
        Handle batch streaming with a callable function.

        Args:
            df (DataFrame): The Streaming DataFrame to process.
            func (Callable[[DataFrame, int], None]): The callable function to apply to each batch.

        Returns:
            StreamingQuery: The streaming query.
        """
        return (
            df.writeStream.trigger(processingTime=f"{DELAY} seconds")
            .foreachBatch(func)
            .start()
        )

    def get_history_df(
        self,
        from_timestamp: Optional[datetime] = None,
        timestamp_col: str = "Timestamp",
    ) -> DataFrame:
        """
        Get the history DataFrame from Kafka.

        Args:
            from_timestamp (Optional[datetime]): The starting timestamp for filtering the data.

        Returns:
            DataFrame: The history DataFrame.
        """

        raw_history_df: DataFrame = (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        history_df: DataFrame = (
            raw_history_df.selectExpr("CAST(value AS STRING)")
            .select(F.from_json("value", ", ".join(self.schema_list)).alias("data"))
            .selectExpr("data.*")
        )

        if from_timestamp:
            if timestamp_col not in [
                col_type.split(" ")[0] for col_type in self.schema_list
            ]:
                raise ValueError(
                    f"The {timestamp_col} column is not in the {self.schema_list}.",
                )

            history_df = history_df.filter(F.col(timestamp_col) >= from_timestamp)

        return history_df


if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("KafkaConsumer")
        .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

    con = Consumer(EVENT_TOPIC, EVENT_SCHEMA_LIST, spark)
    history_df: DataFrame = con.get_history_df()
    print(*history_df.tail(20), sep="\n")

    # # Testing handle_batch_streaming_with_callable
    # def callable_handle_batch(df: DataFrame, epoch_id: int) -> None:
    #     df.show(truncate=False)

    # streaming_dataframe: DataFrame = con.get_streaming_dataframe()
    # streaming_query: StreamingQuery = con.handle_batch_streaming_with_callable(
    #     streaming_dataframe, callable_handle_batch
    # )

    # try:
    #     streaming_query.awaitTermination()
    # except KeyboardInterrupt:
    #     print("Stopping streaming query...")
    #     streaming_query.stop()
    #     print("Streaming query stopped.")
