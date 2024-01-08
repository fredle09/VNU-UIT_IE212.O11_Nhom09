import json
from kafka import KafkaProducer
from config import *
from time import sleep

from pandas import read_csv
from pandas import Series
from pandas import DataFrame


def streaming_data(
    df: DataFrame,
    topic: str,
) -> None:
    topic = topic or STORE_TOPIC

    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    def send_to_kafka(row: Series) -> None:
        record = ",".join([str(val) for val in row.values])
        # print(record)

        producer.send(topic=topic, value=record)
        sleep(DELAY)

    row: Series

    try:
        for _, row in df.iterrows():
            send_to_kafka(row)
    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Closing producer...")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "data_path",
        type=str,
        help="Path to the data to stream.",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=STORE_TOPIC,
        help="Name of the Kafka topic to stream.",
        required=False,
    )

    args = parser.parse_args()

    df: DataFrame = read_csv(args.data_path)

    streaming_data(df, topic=args.topic)
