from typing import List, Union
from kafka import KafkaProducer
from config import *
from time import sleep
from datetime import datetime
import csv


def streaming_data(
    path: str,
    topic: str,
    start_time: str = "",
    debug: bool = False,
) -> None:
    """
    Reads data from a CSV file and streams it to a Kafka topic by simulating a
    real-time data stream.

    Args:
        path (str): The path to the CSV file.
        topic (str): The Kafka topic to which the data will be streamed.
        start_time (str): The start time of the data stream.
        debug (bool): Whether to print debug information.

    Returns:
        None
    """
    topic = topic or STORE_TOPIC

    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: str(x).encode("utf-8"),
    )

    def send_to_kafka(row: List[str]) -> None:
        record = ",".join([str(val) for val in row])
        if debug:
            print(record)

        producer.send(topic=topic, value=record)

    def convert_datetime(row: List[str]) -> datetime:
        FORMAT_DATETIME = "%Y-%m-%d %H:%M"
        res: datetime = datetime.strptime(
            f"{row[2]}-{row[3]}-{row[4]} {row[5]}:{row[6]}", FORMAT_DATETIME
        )
        return res

    def get_time_diff(datetime_1: datetime, datetime_2: datetime) -> float:
        res: float = (datetime_1 - datetime_2).total_seconds() / 15 / 60
        return res

    def send_to_kafka_with_delay(
        previous_time: datetime,
        current_time: datetime,
        row: List[str],
    ) -> None:
        time_diff = get_time_diff(current_time, previous_time)
        sleep(time_diff * DELAY)
        send_to_kafka(row)

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)

            row = next(reader)
            current_time: datetime = convert_datetime(row)
            previous_time: Union[datetime, None] = (
                datetime.strptime(start_time, "%Y-%m-%d") if start_time else None
            )

            if not previous_time:
                send_to_kafka(row)
            else:
                try:
                    while current_time < previous_time:
                        row = next(reader)
                        current_time = convert_datetime(row)
                    send_to_kafka_with_delay(
                        previous_time=previous_time,
                        current_time=current_time,
                        row=row,
                    )
                except StopIteration:
                    raise ValueError(f"Reached end of file before {start_time}")

            for row in reader:
                previous_time = current_time
                current_time = convert_datetime(row)
                send_to_kafka_with_delay(
                    previous_time=previous_time,
                    current_time=current_time,
                    row=row,
                )

            print("Reached end of file")

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
    parser.add_argument(
        "--start-time",
        type=str,
        default="",
        help="Start time of the data stream (with format YYYY-MM-DD).",
        required=False,
    )

    args = parser.parse_args()

    streaming_data(
        path=args.data_path,
        topic=args.topic,
        start_time=args.start_time,
        debug=True,
    )
