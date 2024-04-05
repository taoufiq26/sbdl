from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf


def get_spark_session(env: str) -> SparkSession:
    conf = get_spark_conf(env)

    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=conf) \
            .config('spark.sql.autoBroadcastJoinThreshold', -1) \
            .config('spark.sql.adaptive.enabled', 'false') \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .enableHiveSupport() \
            .getOrCreate()
