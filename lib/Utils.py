from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser

def get_sbdl_conf(env:str):
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('conf/sbdl.conf')
    return config.items(env)

def get_spark_conf(env:str):
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('conf/spark.conf')
    spark_conf = SparkConf()
    for(key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf

def get_spark_session(env:str) -> SparkSession:
    conf = get_spark_conf(env)

    return SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()