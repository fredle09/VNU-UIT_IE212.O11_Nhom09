import pyspark
from config import *

# from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import PolynomialExpansion

from pyspark.ml.regression import LinearRegression

from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types


import findspark
from typing import Literal

findspark.init()

NUMERIC_FEATURES = ["Item_Visibility", "Item_MRP"]
CATEGORY_FEATURES = [
    "Item_Fat_Content",
    "Item_Type",
    "Outlet_Establishment_Year",
    "Outlet_Size",
    "Outlet_Location_Type",
    "Outlet_Type",
]
TARGET = "Item_Outlet_Sales"


def loading_training_data(spark: SparkSession, data_path: str) -> DataFrame:  # type: ignore
    data = spark.read.csv(data_path, header=True, inferSchema=True)
    print(f"Data loaded at {data_path}")
    return data


def preprocessing_data(data: DataFrame) -> DataFrame:
    for feature in NUMERIC_FEATURES:
        if feature not in data.columns:
            raise ValueError(f"{feature} is not in the dataset")

        data = data.withColumn(feature, f.col(feature).cast(types.DoubleType()))

    for feature in CATEGORY_FEATURES:
        if feature not in data.columns:
            raise ValueError(f"{feature} is not in the dataset")

        data = data.withColumn(feature, f.col(feature).cast(types.StringType()))

    print("Data preprocessed")
    return data


def training_model(
    data: DataFrame, target: str = TARGET, predict: str = "prediction", degree: int = 2
) -> PipelineModel:
    numeric_assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES, outputCol="numeric_features"
    )

    polynomial_expansion = PolynomialExpansion(
        inputCol="numeric_features", outputCol="poly_features", degree=degree
    )

    indexers = [
        StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep")
        for col in CATEGORY_FEATURES
    ]

    encoders = [
        OneHotEncoder(inputCol=col + "_index", outputCol=col + "_encoded")
        for col in CATEGORY_FEATURES
    ]

    feature_assembler = VectorAssembler(
        inputCols=["poly_features"] + [col + "_encoded" for col in CATEGORY_FEATURES],
        outputCol="features",
    )

    linear_regression = LinearRegression(
        featuresCol="features", labelCol=target, predictionCol=predict, regParam=0.01
    )

    pipeline: Pipeline = Pipeline(
        stages=[numeric_assembler, polynomial_expansion]
        + indexers
        + encoders
        + [feature_assembler, linear_regression]
    )

    pipeline_model: PipelineModel = pipeline.fit(data)
    print("Model trained")
    return pipeline_model


def saving_model(model: PipelineModel, model_path: str) -> None:
    model.write().overwrite().save(model_path)
    print(f"Model saved at {model_path}")


def predicting_model(model: PipelineModel, input_data: DataFrame) -> DataFrame:
    output_data: DataFrame = model.transform(input_data)
    print("Model predicted")
    return output_data


def evaluating_model(
    model: PipelineModel,
    testing_data: DataFrame,
    predict: str = "prediction",
    target: str = TARGET,
    metric_name: Literal["rmse", "mse", "r2", "mae", "var"] = "rmse",
) -> None:
    evaluator: RegressionEvaluator = RegressionEvaluator(
        labelCol=target, predictionCol=predict, metricName=metric_name
    )

    score: float = evaluator.evaluate(testing_data)
    print(f"{metric_name.upper()}: {score}")


def loading_model(model_path: str) -> PipelineModel:
    model: PipelineModel = PipelineModel.load(model_path)
    print(f"Model loaded from {model_path}")
    return model


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.master("local").appName("Training Model").getOrCreate()  # type: ignore
    data_path: str = "datasets/Train.csv"
    data: DataFrame = loading_training_data(spark, data_path)
    data = preprocessing_data(data)
    model: PipelineModel = training_model(data, degree=5)

    prediction = predicting_model(model, data)
    prediction.select(
        *NUMERIC_FEATURES,
        *CATEGORY_FEATURES,
        TARGET,
        "prediction",
    ).show()

    evaluating_model(model, prediction, metric_name="rmse")
    evaluating_model(model, prediction, metric_name="mae")

    saving_model(model, "models/lr_model")

    model = loading_model("models/lr_model")

    spark.stop()
