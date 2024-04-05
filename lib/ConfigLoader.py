from pyspark import SparkConf
import configparser
import sys


def get_run_env():
    return sys.argv[1].upper()

def get_load_date():
    return sys.argv[2].upper()

def get_config(env:str):
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('conf/sbdl.conf')
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf

def get_spark_conf(env:str):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('conf/spark.conf')
    for(key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
