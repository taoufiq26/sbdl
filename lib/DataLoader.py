from lib import ConfigLoader
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def load_data(spark: SparkSession, path: str, schema:StructType = None) -> DataFrame:
    data = spark.read \
        .format("csv") \
        .option("header", True)
    if schema:
        data.schema(schema)
    else:
        data.option("inferSchema", True)

    return data.load(path=path)


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

def load_accounts(spark: SparkSession) -> DataFrame:
    schema = accounts_schema()
    return load_data(spark, "test_data/accounts/*", schema=schema)


def load_parties(spark: SparkSession) -> DataFrame:
    schema = parties_schema()
    return load_data(spark, path="test_data/parties/*", schema=schema)


def load_adresses(spark: SparkSession) -> DataFrame:
    schema = adress_schema()
    return load_data(spark, path="test_data/party_address/*", schema=schema)
