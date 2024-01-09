import json
from typing import List
from kafka import KafkaProducer
from config import *
from time import sleep

import csv


def streaming_data(path: str, topic: str) -> None:
    """
    Reads data from a CSV file and streams it to a Kafka topic.

    Args:
        path (str): The path to the CSV file.
        topic (str): The Kafka topic to which the data will be streamed.

    Returns:
        None
    """
    topic = topic or STORE_TOPIC

    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    def send_to_kafka(row: List[str]) -> None:
        record = ",".join([str(val) for val in row])
        # print(record)
        # return

        producer.send(topic=topic, value=record)
        sleep(DELAY)

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)
            _ = next(reader)  # skip header
            for row in reader:
                send_to_kafka(row)

    except KeyboardInterrupt:
        print("Interrupted by user")

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

    streaming_data(path=args.data_path, topic=args.topic)
