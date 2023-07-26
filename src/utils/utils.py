import configparser
import logging
from datetime import datetime
from logging import RootLogger
from typing import Dict

from pyspark.sql import SparkSession


def get_logger(file_name: str, stream_output: bool = False) -> RootLogger:
    """
    Function to create a logger based on config provided and returns its object.
    """
    log_file = '{}_{}.log'.format(file_name,
                                  datetime.now().strftime('%Y%m%d%H%M%S%f'))
    logging.getLogger("py4j").setLevel(logging.INFO)
    logging.basicConfig(filename=log_file,
                        format='[%(asctime)s] - %(levelname)s - %(message)s',
                        filemode='a',
                        datefmt='%d-%b-%y %I:%M%p')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if type(stream_output) is bool and stream_output:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s'))
        logging.getLogger('').addHandler(console)
    return logger


def get_spark_session(logger, app_name: str) -> SparkSession:
    """
    Function to create a spark session with the provided app name.
    """
    logger.debug("Inside get_spark_session function in utils.py")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.42.0.0') \
        .getOrCreate()
    return spark


def read_config(logger: RootLogger, config_file: str) -> Dict:
    """
    Function to read the config file and creates a dict object with all config params.
    """
    logger.debug("Inside read_config function in utils.py")
    configs = dict()
    config = configparser.ConfigParser()
    config.read(config_file)
    configs['url_prefix'] = config['jdbc']['url_prefix']
    configs['driver'] = config['jdbc']['driver']
    configs['input_db_file'] = config['input']['db_file']
    configs['leaks_table_name'] = config['input']['leaks_table']
    configs['leak_class_min_date_table_name'] = config['input']['leak_class_min_date_table']
    configs['lut_company_table_name'] = config['input']['lut_company_table']
    configs['town_company_table_name'] = config['input']['town_company_table']
    configs['output_db_file'] = config['output']['db_file']
    configs['output_table'] = config['output']['output_table']
    return configs
