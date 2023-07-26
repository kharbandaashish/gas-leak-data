from pyspark.sql import Row

from src.loaders.loaders import load_data_from_jdbc


def test_load_data_from_jdbc(spark_session):
    url = "jdbc:sqlite:test_data/test_database.sqlite"
    driver = "org.sqlite.JDBC"
    table_name = "test_table"

    expected_df = spark_session.createDataFrame([Row(company_code='AGC', description='A GAS COMPANY'),
                                                 Row(company_code='BGC', description='B GAS COMPANY'),
                                                 Row(company_code='CGC', description='C GAS COMPANY'),
                                                 Row(company_code='DGC', description='D GAS COMPANY'),
                                                 Row(company_code='EGC', description='E GAS COMPANY'),
                                                 Row(company_code='FGC', description='F GAS COMPANY'),
                                                 Row(company_code='GGC', description='G GAS COMPANY'),
                                                 Row(company_code='HGC', description='H GAS COMPANY'),
                                                 Row(company_code='IGC', description='I GAS COMPANY'),
                                                 Row(company_code='JGC', description='J GAS COMPANY')])

    actual_df = load_data_from_jdbc(spark_session, driver, url, table_name)

    assert actual_df.exceptAll(expected_df).collect() == []
    assert expected_df.exceptAll(actual_df).collect() == []
