import os
import shutil
from datetime import datetime

import pyspark.sql.functions as f

from loaders.loaders import load_data_from_jdbc
from utils.utils import get_logger, read_config, get_spark_session
from writers.writers import write_data_to_jdbc


def etl_run() -> None:
    try:
        root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        log_dir = os.path.join(root_dir, "logs")
        conf_dir = os.path.join(root_dir, "conf")
        data_dir = os.path.join(root_dir, "data")
        input_dir = os.path.join(data_dir, "input")
        output_dir = os.path.join(data_dir, "output")

        app_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

        log_file_prefix = os.path.join(log_dir, app_name)
        conf_file = os.path.join(conf_dir, "config.ini")

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

        upsert_flag = False
        final_df = None

        output_path = f"{output_dir}/{output_db_file}"
        prev_output_path = f"{output_dir}/prev_{output_db_file}"

        jdbc_input_url = f"{jdbc_url_prefix}/{input_dir}/{input_db_file}"
        jdbc_output_url = f"{jdbc_url_prefix}{output_dir}/{output_db_file}"
        jdbc_prev_output_url = f"{jdbc_url_prefix}{prev_output_path}"

        # checking if the output already exists to identify if it's a full load or upsert(append/update) load
        if os.path.exists(output_path):
            logger.info("Previous output exists")
            upsert_flag = True
            logger.info(f"Copying: {output_path} to {prev_output_path}")
            shutil.copy(output_path, prev_output_path)

            logger.info("Reading previous leaks output table")
            prev_output_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_prev_output_url, output_table)

            # reading max values for create date and modified date from previous data to identify delta load
            prev_dates = prev_output_df.select(f.max(f.col("create_date")).alias("max_create_date"),
                                               f.max(f.col("modified_date")).alias("max_modified_date")
                                               ).collect()[0]
            load_start_create_date = prev_dates.max_create_date
            load_start_modified_date = prev_dates.max_modified_date
        else:
            logger.info("Previous output not found")
            # setting the dates to 1900-01-01 00:00:00 if no previous output exists
            load_start_create_date = load_start_modified_date = datetime(1900, 1, 1, 0, 0, 0)

        logger.debug(f"upsert_flag: {upsert_flag}")

        logger.info(
            f"load_start_create_date: {load_start_create_date}. load_start_modified_date: {load_start_modified_date}")

        logger.info("Reading leaks table")
        leaks_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, leaks_table_name)

        logger.info("Reading leak_class_min_date table")
        leak_class_min_date_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url,
                                                     leak_class_min_date_table_name)

        logger.info("Reading lut_company table")
        lut_company_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, lut_company_table_name)

        logger.info("Reading town_company table")
        town_company_df = load_data_from_jdbc(logger, spark, jdbc_driver, jdbc_input_url, town_company_table_name)

        # filtering for company_code and selecting required columns and aliasing them as per requirement
        leaks_df1 = leaks_df.where(f.col("company_code") != "ENI") \
            .select("leak_no", "create_date", "rpt_date", "modified_date",
                    f.col("class").alias("current_class"), "pressure",
                    "cover", "town_code", "company_code")

        # renaming column name as per requirement
        leak_class_min_date_df1 = leak_class_min_date_df.withColumnRenamed("class", "original_class")

        # filtering for company_code and renaming column name as per requirement
        lut_company_df1 = lut_company_df.where(f.col("company_code") != "ENI") \
            .withColumnRenamed("description",
                               "company_name")

        # filtering for market_share and dropping duplicates since I observed some duplicates in this table
        town_company_df1 = town_company_df.where(f.col("company_code") != "ENI") \
            .where(f.col("market_share") >= 1) \
            .dropDuplicates()

        if upsert_flag:
            # generating the dataframe for the records that have been added in leaks table
            append_df = leaks_df1.where(f.col("create_date") > load_start_create_date) \
                .join(leak_class_min_date_df1, ["leak_no"], "left") \
                .join(lut_company_df1,
                      ["company_code"],
                      "left") \
                .join(town_company_df1,
                      ["company_code",
                       "town_code"],
                      "left") \
                .select('leak_no', 'create_date', 'rpt_date', 'modified_date', 'current_class',
                        'original_class', 'pressure', 'cover', 'town_code', 'company_code', 'company_name')

            # generating the dataframe for the records that have been modified in leaks table
            updates_df = leaks_df1.where(f.col("modified_date") > load_start_modified_date) \
                .join(leak_class_min_date_df1, ["leak_no"], "left") \
                .join(lut_company_df1,
                      ["company_code"],
                      "left") \
                .join(town_company_df1,
                      ["company_code",
                       "town_code"],
                      "left") \
                .select('leak_no', 'create_date', 'rpt_date', 'modified_date', 'current_class',
                        'original_class', 'pressure', 'cover', 'town_code', 'company_code', 'company_name')

            append_df_count = append_df.count()
            updates_df_count = updates_df.count()

            logger.info(f"Append dataframe count : {append_df_count}")
            logger.info(f"Updates dataframe count : {updates_df_count}")

            if updates_df_count > 0:
                # left semi join to get the records updates df with updated values and ignoring old values
                final_updates_df = updates_df.join(prev_output_df, ["leak_no"], "leftsemi")

                # legt anti join to get old records that have not been modified
                final_prev_df = prev_output_df.join(updates_df.select("leak_no"), ["leak_no"], "left_anti")

                # combining updated and old records
                final_df = final_updates_df.unionAll(final_prev_df)
            if append_df_count > 0:
                # combining new records in final df
                final_df = final_df.unionAll(append_df)
        else:
            # if no previous output found then full load of data
            final_df = leaks_df1 \
                .join(leak_class_min_date_df1, ["leak_no"], "left") \
                .join(lut_company_df1,
                      ["company_code"],
                      "left") \
                .join(town_company_df1,
                      ["company_code",
                       "town_code"],
                      "left") \
                .select('leak_no', 'create_date', 'rpt_date', 'modified_date', 'current_class',
                        'original_class', 'pressure', 'cover', 'town_code', 'company_code', 'company_name')

        if upsert_flag:
            logger.info("Deleting previous output file")
            os.remove(prev_output_path)

        if final_df:
            logger.info(f"Writing output at: {output_dir}/{output_db_file}")
            logger.info(f"Total records: {final_df.count()}")
            # writing final output
            write_data_to_jdbc(logger, final_df, jdbc_driver, jdbc_output_url, output_table)
        else:
            logger.info("No changes to write")
        logger.info("Application execution completed successfully")

    except Exception as e:
        logger.error(e)
