from bin.config import *


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
