from typing import List
import os
from datetime import datetime

from bin.config import *
from bin.streaming_data import streaming_data
from bin.consumer import consumer_data
from multiprocessing import Process, Semaphore


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-time",
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

    if args.start_time:
        print(f"Start time: {args.start_time}")
        datetime.strptime(args.start_time, "%Y-%m-%d")

    csv_file_names = [
        "Bronx_data.csv",
        "Brooklyn_data.csv",
        "Manhattan_data.csv",
        "Queens_data.csv",
        "Staten_Island_data.csv",
    ]

    processes: List[Process] = []
    semaphore_prepare = Semaphore(0)
    semaphore_running = Semaphore(0)

    DATASETS_PATH = "datasets/"
    for csv_file_name in csv_file_names:
        p = Process(
            target=streaming_data,
            kwargs={
                "path": os.path.join(DATASETS_PATH, csv_file_name),
                "topic": "store_tutorial_15",
                "start_time": args.start_time,
                "semaphore_prepare": semaphore_prepare,
                "semaphore_running": semaphore_running,
                "debug": True,
            },
        )
        p.start()
        processes.append(p)

    for _ in range(len(processes)):
        semaphore_prepare.acquire()
    print("All processes are ready to start")
    for _ in range(len(processes)):
        semaphore_running.release()

    print("All processes are running")

    for p in processes:
        p.join()
