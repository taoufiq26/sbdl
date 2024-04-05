from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf


def get_spark_session(env: str) -> SparkSession:
    conf = get_spark_conf(env)

    return SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()