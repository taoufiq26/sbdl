import sys

from pyspark.sql import functions as f
from lib.logger import Log4J
from lib import Utils

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : arguments are missing")

    # Load arguments
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2].upper()

    spark = Utils.get_spark_session(env=job_run_env)

    logger = Log4J(spark)

    logger.info("Starting Application...")

    df = spark.range(0,10)
    df.show()

    logger.info("Stopping Application...")
    spark.stop()




