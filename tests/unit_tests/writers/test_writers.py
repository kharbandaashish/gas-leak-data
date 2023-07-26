from pyspark.sql import Row

from src.writers.writers import write_data_to_jdbc


def test_write_data_to_jdbc(logger, spark_session, data_files_dir):
    url = f"jdbc:sqlite:{data_files_dir}/test_write_database.sqlite"
    driver = "org.sqlite.JDBC"
    table_name = "test_table"

    df = spark_session.createDataFrame([Row(company_code='AGC', description='A GAS COMPANY'),
                                        Row(company_code='BGC', description='B GAS COMPANY'),
                                        Row(company_code='CGC', description='C GAS COMPANY'),
                                        Row(company_code='DGC', description='D GAS COMPANY'),
                                        Row(company_code='EGC', description='E GAS COMPANY'),
                                        Row(company_code='FGC', description='F GAS COMPANY'),
                                        Row(company_code='GGC', description='G GAS COMPANY'),
                                        Row(company_code='HGC', description='H GAS COMPANY'),
                                        Row(company_code='IGC', description='I GAS COMPANY'),
                                        Row(company_code='JGC', description='J GAS COMPANY')])

    write_data_to_jdbc(logger, df, driver, url, table_name)

    actual_df = spark_session.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable",
                                                                                                     table_name).load()

    assert actual_df.exceptAll(df).collect() == []
    assert df.exceptAll(actual_df).collect() == []
