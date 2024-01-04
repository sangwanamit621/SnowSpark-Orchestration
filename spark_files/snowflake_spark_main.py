# Note: We will upload this file in gcs and provide the gcs path in Airflow task

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from argparse import ArgumentParser

def main(snowflake_configs: dict, file_date:str=None):
    """
    This function processes the daily orders data and udpates the target table in snowflake with records from daily orders data where order_status is Completed
    Input Parameters:
    snowflake_configs(dict): important snowflake related configurations to connect with snowflake table
    file_date(str): Used if we want to process orders data file of a specific date. Default is None so we will process last_day's orders data file.
    Format of snowflake_configs dict is:
    {
    "sfURL": "https://potjr-mh12308.snowflakecomputing.com",
    "sfAccount":"potjr-mh12308",
    "sfUser":"amitsan",
    "sfPassword":"Amit@123",
    "sfDatabase":"snowpipe_project",
    "sfSchema":"PUBLIC",
    "sfWarehouse":"ORDERS_DW",
    "truncate_table":"off",
    "sfRole":"ACCOUNTADMIN"
    }
    """
    spark = SparkSession.builder.appName("spark-snowflake")\
            .config("spark.jars","gs://jars-spark/snowflake-jdbc-3.13.30.jar,gs://jars-spark/spark-snowflake_2.12-2.12.0-spark_3.3.jar")\
            .getOrCreate()

    print("Spark Session Created")

    file_path = f"gs://snowflake_621/orders_data/orders_{file_date}.csv"

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    # read data from snowflake target table
    df_target_table = spark.read.format(SNOWFLAKE_SOURCE_NAME)\
                            .options(**snowflake_configs)\
                            .option("dbtable","completed_orders_data")\
                            .load()

    print("Data read successful from snowflake")
    df_target_table.show(5)


    # read data from gcs
    df_gcs = spark.read.format("csv")\
                    .option("header","true")\
                    .option("inferSchema","true")\
                    .load(file_path)

    print("Data read successful from gcs bucket")
    df_gcs.show(5)

    df_gcs_completed = df_gcs.where(col("order_status")=="Completed")

    # Filtering order_ids for which product got returned by customer
    records_to_remove_from_target = df_gcs.where(col("order_status")=="Returned").rdd.map(lambda df:df["order_id"]).collect()

    # Removing the record for which product got returned
    df_target_table = df_target_table.where(~col("order_id").isin(records_to_remove_from_target))

    df_final_target = df_target_table.union(df_gcs_completed)
    print("Final data created for storing in snowflake table")

    # updating the snowflake table by overwriting the data
    df_final_target.write.format(SNOWFLAKE_SOURCE_NAME)\
                    .options(**snowflake_configs)\
                    .option("dbtable","completed_orders_data")\
                    .mode("overwrite")\
                    .save()
    print("Data write successful in snowflake table")

    spark.stop()
    print("Spark Session closed")


if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument("--sf_config", type=dict, required=True, help="Configurations for snowflake to access the snowflake table and datawarehouse")
    parser.add_argument("--file_date", type=str, required=False, help="Date of file for which we want to process the orders data", default=None)

    args = parser.parse_args()
    snowflake_configs = args.sf_config
    file_date = args.file_date

    main(snowflake_configs=snowflake_configs, file_date=file_date)
