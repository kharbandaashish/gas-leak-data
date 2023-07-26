from typing import Generator

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def data_files_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("data")


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Tests")
        .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.42.0.0')\
    ).getOrCreate()
    yield spark
    spark.stop()
