SCALE_VERSION = "2.13"
SPARK_VERSION = "3.5.0"
KAFKA_VERSION = "3.6.1"

SPARK_PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALE_VERSION}:{SPARK_VERSION}",
    f"org.apache.kafka:kafka-clients:{KAFKA_VERSION}",
]

DELAY = 10  # seconds
NUM_PARTITIONS = 3
KAFKA_BROKER = "localhost:9092"
STORE_TOPIC = "store_tutorial_15"
STORE_CONSUMER_GROUP = "store_tutorial_15"
PREDICTION_TOPIC = "prediction_tutorial_15"
PREDICTION_CONSUMER_GROUP = "prediction_tutorial_15"
BATCH_FOLDER = "batch_folder/"
JSON_SCHEMA_LIST = [
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
