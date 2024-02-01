import os
from time import sleep, time
from json import dumps as json_dumps
from datetime import datetime, timedelta
from typing import Optional, Callable, Any

import pandas as pd
import numpy as np

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

import findspark

findspark.init()

SCALE_VERSION = "2.13"
SPARK_VERSION = "3.5.0"
KAFKA_VERSION = "3.6.1"

SPARK_PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALE_VERSION}:{SPARK_VERSION}",
    f"org.apache.kafka:kafka-clients:{KAFKA_VERSION}",
]

pd.options.mode.copy_on_write = True

DELAY = 20  # seconds
EXPIRE_TIME = 3  # hours
DATASETS_PATH = "datasets/"
BATCH_FOLDER = "batch_folder/"
KAFKA_BROKER = "localhost:9092"
STORE_TOPIC = "STORE_TOPIC_"
# EVENT_TOPIC = "EVENT_TOPIC_"
EVENT_TOPIC = "TestStore"
# SEGMENT_TOPIC = "SEGMENT_TOPIC_"
SEGMENT_TOPIC = "TestSegment"
# PREDICTION_TOPIC = "PREDICTION_TOPIC_"
PREDICTION_TOPIC = "TestPrediction"

STORE_SCHEMA_LIST = [
    "RequestID INT",
    "Boro STRING",
    "Yr INT",
    "M INT",
    "D INT",
    "HH INT",
    "MM INT",
    "Vol INT",
    "SegmentID INT",
    "WktGeom STRING",
    "street STRING",
    "fromSt STRING",
    "toSt STRING",
    "Direction STRING",
]

EVENT_SCHEMA_LIST = [
    "SegmentID INT",
    "Direction STRING",
    "Vol INT",
    "Timestamp TIMESTAMP",
]

SEGMENT_SCHEMA_LIST = [
    "SegmentID INT",
    "Boro STRING",
    "street STRING",
    "fromSt STRING",
    "toSt STRING",
    "Lat DOUBLE",
    "Long DOUBLE",
]

PREDICTION_SCHEMA_LIST = [
    "SegmentID INT",
    "Direction STRING",
    "ds TIMESTAMP",
    "yhat DOUBLE",
    "min_history_ds TIMESTAMP",
    "max_history_ds TIMESTAMP",
]
