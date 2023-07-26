from logging import RootLogger

from pyspark.sql import DataFrame


def write_data_to_jdbc(logger: RootLogger, df: DataFrame, driver: str, url: str, table_name: str) -> None:
    """
    Function to write the dataframe as a table in jdbc using spark
    """
    logger.debug("Inside write_data_to_jdbc function in writers.py")
    df.write.mode("overwrite") \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("dbtable", table_name) \
        .save()
