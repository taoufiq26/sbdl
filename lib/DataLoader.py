from lib import ConfigLoader
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def load_data(spark: SparkSession, path: str, schema:StructType, runtime_filter) -> DataFrame:
    return spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load(path=path) \
        .where(runtime_filter)



def accounts_schema() -> StructType:
    return StructType([
        StructField('load_date', DateType()),
        StructField('active_ind', IntegerType()),
        StructField('account_id', LongType()),
        StructField('source_sys', StringType()),
        StructField('account_start_date', TimestampType()),
        StructField('legal_title_1', StringType()),
        StructField('legal_title_2', StringType()),
        StructField('tax_id_type', StringType()),
        StructField('tax_id', StringType()),
        StructField('branch_code', StringType()),
        StructField('country', StringType())
    ])

def parties_schema() -> StructType:
    return StructType([
        StructField('load_date', DateType()),
        StructField('account_id', LongType()),
        StructField('party_id', LongType()),
        StructField('relation_type', StringType()),
        StructField('relation_start_date', TimestampType())
    ])

def adress_schema() -> StructType:
    return StructType([
        StructField('load_date', DateType()),
        StructField('party_id', LongType()),
        StructField('address_line_1', StringType()),
        StructField('address_line_2', StringType()),
        StructField('city', StringType()),
        StructField('postal_code', IntegerType()),
        StructField('country_of_address', StringType()),
        StructField('address_start_date', TimestampType())
    ])

def load_accounts(spark: SparkSession, env, enable_hive, hive_db) -> DataFrame:
    runtime_filter = ConfigLoader.get_data_filter(env, "account.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".accounts").where(runtime_filter)
    else:
        schema = accounts_schema()
        return load_data(spark, "test_data/accounts/*", schema=schema, runtime_filter=runtime_filter)


def load_parties(spark: SparkSession, env, enable_hive, hive_db) -> DataFrame:
    runtime_filter = ConfigLoader.get_data_filter(env, "party.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".parties").where(runtime_filter)
    else:
        schema = parties_schema()
        return load_data(spark, path="test_data/parties/*", schema=schema, runtime_filter=runtime_filter)


def load_adresses(spark: SparkSession, env, enable_hive, hive_db) -> DataFrame:
    runtime_filter = ConfigLoader.get_data_filter(env, "address.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".party_address").where(runtime_filter)
    else:
        schema = adress_schema()
        return load_data(spark, path="test_data/party_address/*", schema=schema, runtime_filter=runtime_filter)
