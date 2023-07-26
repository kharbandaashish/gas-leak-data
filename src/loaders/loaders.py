from logging import RootLogger

from pyspark.sql import SparkSession, DataFrame


def load_data_from_jdbc(logger: RootLogger, spark: SparkSession, driver: str, url: str, table_name: str) -> DataFrame:
    """
    Function to read a table from jdbc using spark and return a dataframe
    """
    logger.debug("Inside load_data_from_jdbc function in loaders.py")
    df = spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table_name).load()
    return df
