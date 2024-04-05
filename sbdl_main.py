import sys

from pyspark.sql.functions import to_json, struct, col

from lib.logger import Log4J
from lib import Utils
from lib import DataLoader, Transformations

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : arguments are missing")

    # Load arguments
    job_run_env = sys.argv[1].upper()
    job_load_date = sys.argv[2].upper()

    # Init Spark session
    spark = Utils.get_spark_session(env=job_run_env)

    # Init logger
    logger = Log4J(spark)

    logger.info("Starting Application...")

    # Load Raw Data and transform structure
    logger.info("Reading SBDL Account DF")
    acounts_df = DataLoader.load_accounts(spark)
    contract_df = Transformations.get_contract(acounts_df)

    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.load_parties(spark)
    relations_df = Transformations.get_relations(parties_df)

    logger.info("Reading SBDL Address DF")
    adress_df = DataLoader.load_adresses(spark)
    relation_address_df = Transformations.get_address(adress_df)


    # Joins Dataframes
    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)

    logger.info("Join Contracts and  Party Address")
    data_df = Transformations.join_contract_party(contract_df, party_address_df)

    # Create final DF with headers
    logger.info("Apply Header and create Event")
    final_df = Transformations.apply_header(spark, data_df)

    # Write Results Data to kafka
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))

    kafka_kv_df.write.format("json").mode("overwrite").save("test_data/results/kafka_df/")

    input("Press enter to stop application...")
    logger.info("Stopping Application...")
    spark.stop()




