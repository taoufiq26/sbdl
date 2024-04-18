import sys
import uuid

from pyspark.sql.functions import to_json, struct, col
from pyspark.sql.types import StringType

from lib.logger import Log4J
from lib import Utils, ConfigLoader
from lib import DataLoader, Transformations

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : arguments are missing")

    # Load arguments
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2].upper()
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]

    # Init Spark session
    spark = Utils.get_spark_session(env=job_run_env)

    # Init logger
    logger = Log4J(spark)

    logger.info("Starting Application...")

    # Load Raw Data and transform structure
    logger.info("Reading SBDL Account DF")
    acounts_df = DataLoader.load_accounts(spark, job_run_env, enable_hive, hive_db)
    contract_df = Transformations.get_contract(acounts_df)

    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.load_parties(spark, job_run_env, enable_hive, hive_db)
    relations_df = Transformations.get_relations(parties_df)

    logger.info("Reading SBDL Address DF")
    adress_df = DataLoader.load_adresses(spark, job_run_env, enable_hive, hive_db)
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
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key").cast(StringType()),
                                  to_json(struct("*")).alias("value"))

    # kafka_kv_df.write.format("json").mode("overwrite").save("test_data/results/kafka_df/")
    # Write Dataframe to Kafka Topic
    logger.info("Sending data to Kafka...")
    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]
    logger.info(kafka_kv_df.printSchema())


    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()

    logger.info("Finished sendingdata to Kafka")



    input("Press enter to stop application...")
    logger.info("Stopping Application...")
    spark.stop()




