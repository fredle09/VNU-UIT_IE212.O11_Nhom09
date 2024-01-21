from typing import Union
from time import sleep, time
from json import dumps as json_dumps
from datetime import datetime
import csv

from kafka import KafkaProducer

from bin.config import *

JSON_COLS: list[str] = []
JSON_DATA_TYPES: dict[str, type] = {}
for col_info in JSON_SCHEMA_LIST:
    col_name, col_type = col_info.split(" ")
    JSON_COLS.append(col_name)
    JSON_DATA_TYPES[col_name] = int if col_type == "INT" else str


def streaming_data(
    path: str,
    topic: str = STORE_TOPIC,  # type: ignore
    start_date: Union[str, None] = None,
    semaphore_prepare=None,
    semaphore_running=None,
    debug: bool = False,
) -> None:
    """
    Reads data from a CSV file and streams it to a Kafka topic by simulating a
    real-time data stream.

    Args:
        path (str): The path to the CSV file.
        topic (str): The Kafka topic to which the data will be streamed.
        start_date (str, optional): The start time of the data stream.
        semaphore_prepare (Event, optional): A semaphore used for synchronization
            before starting the data stream.
        semaphore_running (Event, optional): A semaphore used for synchronization
            to indicate that the data stream is running.
        debug (bool, optional): Whether to print debug information.

    Returns:
        None
    """
    topic = topic or STORE_TOPIC
    if start_date and (not semaphore_prepare or not semaphore_running):
        raise ValueError(
            "Invalid arguments\nif start_date is not None, then semaphore_prepare"
            + "and semaphore_running must not be None"
        )

    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json_dumps(x).encode("utf-8"),
    )

    def send_to_kafka(row: list[str]) -> None:
        record: dict = {}
        for col_name, col_type in JSON_DATA_TYPES.items():
            record[col_name] = col_type(row[JSON_COLS.index(col_name)])

        if debug:
            print(record)

        producer.send(topic=topic, value=record)

    def convert_datetime(row: list[str]) -> datetime:
        FORMAT_DATETIME = "%Y-%m-%d %H:%M"
        res: datetime = datetime.strptime(
            f"{row[2]}-{row[3]}-{row[4]} {row[5]}:{row[6]}", FORMAT_DATETIME
        )
        return res

    def get_time_diff(datetime_1: datetime, datetime_2: datetime) -> float:
        res: float = (datetime_1 - datetime_2).total_seconds() / 15 / 60
        return res

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)

            row = next(reader)
            current_time: datetime = convert_datetime(row)
            previous_time: Union[datetime, None] = (
                datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
            )

            # Finding a row can stream after the start time
            while previous_time and current_time < previous_time:
                # Handle the case where the start time is after the last row
                try:
                    row = next(reader)
                except StopIteration:
                    if semaphore_prepare:
                        semaphore_prepare.release()
                    print(f"Reached end of file before {start_date}")
                    return
                # Update the current time of row
                current_time = convert_datetime(row)

            # Ensure that all the processes stream at the same time if user pass
            # the start time
            if semaphore_prepare and semaphore_running:
                semaphore_prepare.release()
                semaphore_running.acquire()

            # Ensure that the first data is streamed at the start time
            if previous_time and current_time > previous_time:
                time_diff: float = get_time_diff(current_time, previous_time)
                sleep(time_diff * DELAY)

            # Streaming data from the current row
            while True:
                # Set time point to calculate the time to sleep
                time_point: float = time()
                # Handle the case where the next data has the same time as the
                # current data
                while True:
                    send_to_kafka(row)
                    # Handle the case where the streaming all the data
                    try:
                        row = next(reader)
                    except StopIteration:
                        print("Reached end of file")
                        return
                    # Break if the next data has the different time
                    if convert_datetime(row) > current_time:
                        break

                # Update the previous time and current time
                previous_time = current_time
                current_time = convert_datetime(row)
                diff_time: float = get_time_diff(current_time, previous_time)
                time_process: float = time() - time_point
                # Sleep to simulate the real-time data stream
                sleep(diff_time * DELAY - time_process)

    except KeyboardInterrupt:
        pass

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
        start_date=args.start_date,
        debug=True,
    )
