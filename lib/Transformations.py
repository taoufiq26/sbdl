from pyspark.sql.functions import *
from pyspark.sql import DataFrame, SparkSession


def get_insert_operation (column: Column, column_name) -> Column:
    return struct(
        lit('INSERT').alias("operation"),
        lit(None).alias("oldValue"),
        column.alias("newValue")
    ).alias(column_name)


def get_address (df: DataFrame):
    adress = struct(
        col("address_line_1").alias("addressLine1"),
        col("address_line_2").alias("addressLine2"),
        col("city").alias("addressCity"),
        col("postal_code").alias("addressPostalCode"),
        col("country_of_address").alias("addressCountry"),
        col("address_start_date").alias("addressStartDate"),
    )
    return df.select("party_id", get_insert_operation(adress, "partyAddress"))


def get_relations (df: DataFrame):
    return df.select(
        "account_id",
        "party_id",
        get_insert_operation(col('party_id'), "partyIdentifier"),
        get_insert_operation(col('relation_type'), "partyRelationshipType"),
        get_insert_operation(col('relation_start_date'), "partyRelationStartDateTime")
    )


def get_contract (df: DataFrame):
    contract_title = array(when(~isnull("legal_title_1"),
                                   struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                          col("legal_title_1").alias("contractTitleLine"))),
                              when(~isnull("legal_title_2"),
                                   struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                          col("legal_title_2").alias("contractTitleLine")))
                              )
    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(
        col("tax_id_type").alias("taxIdType"),
        col("tax_id").alias("taxId"),
    )

    return df.select(
        "account_id",
        get_insert_operation(col('account_id'), "contractIdentifier"),
        get_insert_operation(col('source_sys'), "sourceSystemIdentifier"),
        get_insert_operation(col('account_start_date'), "contactStartDateTime"),
        get_insert_operation(tax_identifier, "taxIdentifier"),
        get_insert_operation(contract_title_nl, "contractTitle"),
        get_insert_operation(col('branch_code'), "contractBranchCode"),
        get_insert_operation(col('country'), "contractCountry")
    )


def apply_header (spark: SparkSession, df):
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
        .select(struct(
                    expr("uuid()").alias("eventIdentifier"),
                    "eventType",
                    "majorSchemaVersion",
                    "minorSchemaVersion",
                    lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                    ).alias("eventHeader"),
                array(struct(
                    lit("contractIdentifier").alias("keyField"),
                    col("account_id").alias("keyValue"))).alias("keys"),
                struct(
                    "contractIdentifier",
                    "sourceSystemIdentifier",
                    "contactStartDateTime",
                    "taxIdentifier",
                    "contractTitle",
                    "contractBranchCode",
                    "contractCountry",
                    "partyRelations",
                ).alias("payload"))
    return event_df


def join_party_address (relations_df: DataFrame, relation_address_df:DataFrame) -> DataFrame:
    return relations_df.join(relation_address_df, "party_id", 'left_outer') \
        .groupby("account_id") \
        .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))

def join_contract_party(contract_df: DataFrame, party_address_df:DataFrame) -> DataFrame:
    return contract_df.join(party_address_df, "account_id", 'left_outer')