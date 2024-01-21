import os
from datetime import datetime
from multiprocessing import Process, Semaphore

from bin.config import *
from bin.streaming_data import streaming_data

DATASETS_PATH = "datasets/"


def validate_datetime(date_text: str, debug: bool = False) -> None:
    """
    Validates a string as a date.

    Args:
        date_text (str): The date to validate.

    Returns:
        None
    """
    try:
        if debug:
            print(f"Validating date: {date_text}")
        datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-date",
        type=str,
        help="Path to the data file",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug messages",
        default=False,
    )

    args = parser.parse_args()

    # Validate start time
    if args.start_date:
        validate_datetime(args.start_date)

    csv_file_names = [
        "Bronx_data.csv",
        "Brooklyn_data.csv",
        "Manhattan_data.csv",
        "Queens_data.csv",
        "Staten_Island_data.csv",
    ]

    processes: list[Process] = []

    semaphore_prepare = Semaphore(0)
    semaphore_running = Semaphore(0)

    # Start streaming processes
    for csv_file_name in csv_file_names:
        p = Process(
            target=streaming_data,
            kwargs={
                "path": os.path.join(DATASETS_PATH, csv_file_name),
                "topic": STORE_TOPIC,
                "start_date": args.start_date,
                "semaphore_prepare": semaphore_prepare,
                "semaphore_running": semaphore_running,
                "debug": args.debug,
            },
        )
        p.start()
        processes.append(p)

    # Wait for all processes to be ready and running
    for _ in range(len(processes)):
        semaphore_prepare.acquire()
    if args.debug:
        print("Waiting all of processes are ready to start")

    for _ in range(len(processes)):
        semaphore_running.release()
    if args.debug:
        print("All processes are running")

    # Wait for all processes to finish
    # and handle KeyboardInterrupt
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Stopping streaming processes...")
