from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from config import (
    KAFKA_BROKER,
    KAFKA_VERSION,
    SCALE_VERSION,
    SPARK_VERSION,
    STORE_TOPIC,
)


def consumer_data(
    spark: SparkSession,
    topic: str = STORE_TOPIC,
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
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest" if latest else "earliest")
        .load()
    )

    res = (
        res.selectExpr("CAST(value AS STRING)")
        .select(f.split("value", ",").alias("csv_values"))
        .selectExpr(
            "CAST(csv_values[0] AS INT) as RequestID",
            "CAST(csv_values[1] AS STRING) as Boro",
            "CAST(csv_values[2] AS INT) as Yr",
            "CAST(csv_values[3] AS INT) as M",
            "CAST(csv_values[4] AS INT) as D",
            "CAST(csv_values[5] AS INT) as HH",
            "CAST(csv_values[6] AS INT) as MM",
            "CAST(csv_values[7] AS INT) as Vol",
            "CAST(csv_values[8] AS INT) as SegmentID",
            "CAST(csv_values[9] AS STRING) as WktGeom",
            "CAST(csv_values[10] AS STRING) as street",
            "CAST(csv_values[11] AS STRING) as fromSt",
            "CAST(csv_values[12] AS STRING) as toSt",
            "CAST(csv_values[13] AS STRING) as Direction",
        )
    )

    return res


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--topic",
        type=str,
        default=STORE_TOPIC,
        help="Name of the Kafka topic to stream.",
        required=False,
    )

    args = parser.parse_args()

    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALE_VERSION}:{SPARK_VERSION}",
        f"org.apache.kafka:kafka-clients:{KAFKA_VERSION}",
    ]

    spark: SparkSession = (
        SparkSession.builder.appName("KafkaConsumer")  # type: ignore
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    streaming_df: DataFrame = consumer_data(
        spark,
        topic=args.topic,
    )

    streaming_writer = (
        streaming_df.writeStream.queryName("table_1")
        .trigger(processingTime="5 seconds")
        .outputMode("append")
        .format("memory")
    )

    streaming_writer.start()

    from time import sleep

    try:
        while True:
            spark.sql("SELECT * FROM `table_1`").show(truncate=False)
            sleep(5)

    except KeyboardInterrupt:
        print("Interrupted by user")
