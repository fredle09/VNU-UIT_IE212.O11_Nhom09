import os
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.streaming.query import StreamingQuery

from config import *


def consumer_data_real_time(
    spark: SparkSession,
    topic: str = STORE_TOPIC,  # type: ignore
    latest: bool = True,
) -> DataFrame:
    """
    Reads data from a Kafka topic using SparkSession and returns a DataFrame.

    Args:
        spark (SparkSession): The SparkSession object.
        topic (str, optional): The Kafka topic to read from. Defaults to STORE_TOPIC.
        latest (bool, optional): Whether to start reading from the latest offset. Defaults to True.

    Returns:
        DataFrame: The DataFrame containing the read data.
    """
    topic = topic or STORE_TOPIC

    res: DataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)  # type: ignore
        .option("subscribe", topic)
        .option("startingOffsets", "latest" if latest else "earliest")
        .load()
    )

    res = (
        res.selectExpr("CAST(value AS STRING)")
        .select(f.from_json("value", ", ".join(JSON_SCHEMA_LIST)).alias("data"))  # type: ignore
        .selectExpr("data.*")
    )

    return res


def consumer_data_batch_with_handle(
    spark: SparkSession,
    func: Callable[[DataFrame, int], None],
    topic: str = STORE_TOPIC,  # type: ignore
    latest: bool = True,
) -> StreamingQuery:
    """
    Consume data from a Kafka topic in batch mode and process it using the provided function.

    Args:
        spark (SparkSession): The Spark session.
        func (Callable[[DataFrame, int], None]): The function to process the data batch.
        topic (str, optional): The Kafka topic to consume from. Defaults to STORE_TOPIC.
        latest (bool, optional): Whether to consume the latest data or not. Defaults to True.

    Returns:
        StreamingQuery: The streaming query object representing the running query.
    """
    res: StreamingQuery
    streaming_df: DataFrame = consumer_data_real_time(spark, topic, latest)
    res = (
        streaming_df.writeStream.trigger(processingTime=f"{DELAY} seconds")
        .foreachBatch(func)
        .start()
    )

    return res


def write_batch_into_csv(df: DataFrame, epoch_id: int) -> None:
    """
    Writes a batch of data into csv file.

    Args:
        df (DataFrame): The DataFrame containing the data to write.
        epoch_id (int): The epoch ID.

    Returns:
        None
    """
    if not os.path.exists(BATCH_FOLDER):
        os.makedirs(BATCH_FOLDER)

    df.toPandas().to_csv(
        f"{BATCH_FOLDER}batch.csv",
        header=True,
        mode="w+",
        index=False,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--topic",
        type=str,
        default=STORE_TOPIC,  # type: ignore
        help="Name of the Kafka topic to stream.",
        required=False,
    )
    args = parser.parse_args()

    spark: SparkSession = (
        SparkSession.builder.appName("KafkaConsumer")  # type: ignore
        .config("spark.jars.packages", ",".join(SPARK_PACKAGES))  # type: ignore
        .getOrCreate()
    )

    streaming_query = consumer_data_batch_with_handle(
        spark=spark,
        func=write_batch_into_csv,
        topic=args.topic,
    )

    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        streaming_query.stop()
        print("Streaming query stopped.")
