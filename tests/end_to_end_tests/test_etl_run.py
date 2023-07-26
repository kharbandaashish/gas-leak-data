import os

import pyspark.sql.functions as f
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType

from src.etl_run import etl_run


def test_etl_run(spark_session):
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

    input_url = f"jdbc:sqlite:{root_dir}/data/input/database.sqlite"
    output_url = f"jdbc:sqlite:{root_dir}/data/output/database.sqlite"
    driver = "org.sqlite.JDBC"
    input_leaks_table_name = "leaks"
    output_table_name = "leaks_output"

    etl_run()

    df = spark_session.read.format("jdbc") \
        .option("driver", driver) \
        .option("url", input_url) \
        .option("dbtable", input_leaks_table_name).load()

    expected_schema = StructType(
        [
            StructField('leak_no', IntegerType(), True),
            StructField('create_date', TimestampType(), True),
            StructField('rpt_date', TimestampType(), True),
            StructField('modified_date', TimestampType(), True),
            StructField('current_class', StringType(), True),
            StructField('original_class', StringType(), True),
            StructField('pressure', StringType(), True),
            StructField('cover', StringType(), True),
            StructField('town_code', StringType(), True),
            StructField('company_code', StringType(), True),
            StructField('company_name', StringType(), True)
        ]
    )

    actual_df = spark_session.read.format("jdbc") \
        .option("driver", driver) \
        .option("url", output_url) \
        .option("dbtable", output_table_name).load()

    assert actual_df.columns == ['leak_no', 'create_date', 'rpt_date', 'modified_date', 'current_class',
                                'original_class', 'pressure', 'cover', 'town_code', 'company_code', 'company_name']
    assert df.count() == actual_df.count()
    assert actual_df.where(f.col("create_date") < f.col("rpt_date")).collect() == []
