import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *


EVENTS_SCHEMA = StructType([
    StructField("GlobalEventID", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", DoubleType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", LongType(), True),   # era DoubleType
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", DoubleType(), True),
    StructField("Actor1Geo_Type", StringType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_Lat", DoubleType(), True),
    StructField("Actor1Geo_Long", LongType(), True),    # era DoubleType
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", StringType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_Lat", DoubleType(), True),
    StructField("Actor2Geo_Long", DoubleType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", StringType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_Lat", DoubleType(), True),
    StructField("ActionGeo_Long", DoubleType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", StringType(), True),
    StructField("SOURCEURL", StringType(), True),
])


def get_args():
    parser = argparse.ArgumentParser(description="GDELT Spark Job")
    parser.add_argument("--parquet-dir", default=os.getenv("PARQUET_INPUT_DIR", "/data/parquet"))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/gdelt"))
    return parser.parse_args()


def create_session(app_name: str, mongo_uri: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def read_events(spark, parquet_dir: str):
    """Lee events con schema explícito para evitar conflictos de tipos."""
    return (
        spark.read
        .schema(EVENTS_SCHEMA)
        .parquet(f"{parquet_dir}/events")
    )


def write_to_mongo(df, collection: str, mode: str = "overwrite") -> None:
    (
        df.write
        .format("mongodb")
        .mode(mode)
        .option("collection", collection)
        .save()
    )