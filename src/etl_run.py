import os

import pyspark.sql.functions as f

from loaders.loaders import load_data_from_jdbc
from utils.utils import get_logger, read_config, get_spark_session
from writers.writers import write_data_to_jdbc

curr_dir = os.path.dirname(os.path.realpath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
log_dir = os.path.join(root_dir, "logs")
conf_dir = os.path.join(root_dir, "conf")
data_dir = os.path.join(root_dir, "data")
input_dir = os.path.join(data_dir, "input")
output_dir = os.path.join(data_dir, "output")

app_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
log_file_prefix = os.path.join(log_dir, app_name)
conf_file = os.path.join(conf_dir, "config.ini")


def etl_run() -> None:
    try:
        # checking and creating directories
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        logger = get_logger(log_file_prefix, True)
        logger.info("Application - '{}' started".format(app_name))

        logger.info("Reading configuration file")

        # reading configs
        conf = read_config(logger, conf_file)
        jdbc_url_prefix = conf['url_prefix']
        jdbc_driver = conf['driver']
        input_db_file = conf['input_db_file']
        leaks_table_name = conf['leaks_table_name']
        leak_class_min_date_table_name = conf['leak_class_min_date_table_name']
        lut_company_table_name = conf['lut_company_table_name']
        town_company_table_name = conf['town_company_table_name']
        output_db_file = conf['output_db_file']
        output_table = conf['output_table']

        logger.info("Creating spark session with app-name - '{}'".format(app_name))

        spark = get_spark_session(logger, app_name)

        jdbc_input_url = f"{jdbc_url_prefix}/{input_dir}/{input_db_file}"

        logger.info("Reading leaks table")
        leaks_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, leaks_table_name)

        logger.info("Reading leak_class_min_date table")
        leak_class_min_date_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url,
                                                     leak_class_min_date_table_name)

        logger.info("Reading lut_company table")
        lut_company_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, lut_company_table_name)

        logger.info("Reading town_company table")
        town_company_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, town_company_table_name)

        leaks_df1 = leaks_df.where(f.col("company_code") != "ENI") \
            .select("leak_no", "create_date", "rpt_date", "modified_date",
                    f.col("class").alias("current_class"), "pressure",
                    "cover", "town_code", "company_code")

        leak_class_min_date_df1 = leak_class_min_date_df.withColumnRenamed("class", "original_class")

        lut_company_df1 = lut_company_df.where(f.col("company_code") != "ENI") \
            .withColumnRenamed("description",
                               "company_name")

        town_company_df1 = town_company_df.where(f.col("company_code") != "ENI") \
            .where(f.col("market_share") >= 1).dropDuplicates()

        joined_df = leaks_df1.join(leak_class_min_date_df1, ["leak_no"], "left") \
            .join(lut_company_df1,
                  ["company_code"],
                  "left") \
            .join(town_company_df1,
                  ["company_code",
                   "town_code"],
                  "left")

        final_df = joined_df.select('leak_no', 'create_date', 'rpt_date', 'modified_date', 'current_class',
                                    'original_class', 'pressure', 'cover', 'town_code', 'company_code', 'company_name')

        jdbc_output_url = f"{jdbc_url_prefix}/{output_dir}/{output_db_file}"

        logger.info(f"Writing output at: {output_dir}/{output_db_file}")
        logger.info(f"Total records: {final_df.count()}")

        write_data_to_jdbc(logger, final_df, jdbc_driver, jdbc_output_url, output_table)

    except Exception as e:
        logger.error(e)
