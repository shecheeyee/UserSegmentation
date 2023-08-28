from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, DoubleType

import os
import sys


# Create a Spark session
spark = SparkSession.builder \
    .appName("CsvToParquet") \
    .master("local") \
    .getOrCreate()

args = sys.argv[1:]

date = args[0]
date = date[1:-1].split(", ")


user_date = date[0][1:-1]
promo_date = date[1][1:-1]
tx_date = date[2][1:-1]


src_path = '/opt/airflow/data_/source'
dst_path = '/opt/airflow/data_/destination'

users_dir = src_path + '/users/' + user_date
promo_dir = src_path + '/promotions/' + promo_date
tx_dir = src_path + '/transactions/' + tx_date


def csv_to_parquet(daily_dir_path):
    """_summary_

    Args:
        daily_dir_path (_type_): takes in the directory (full absolute path) to daily users/promo/tx's



    Writes into parquet file if date exists/ is updated
    """
    try:
        csv_path = daily_dir_path + "/" + \
            [file for file in os.listdir(
                daily_dir_path) if file.endswith('.csv')][0]
        df = spark.read.option("sep", "\t").option(
            "inferSchema", "True").option("header", "true").csv(csv_path)
        table, date = daily_dir_path.split(
            "/")[-2], daily_dir_path.split("/")[-1]
        # print(table)
        # print(date)
        print("Writing to parquet file...")
        df.write.mode("overwrite").parquet(dst_path + '/' + table + '/' + date)

    except FileNotFoundError:
        print(daily_dir_path)
        print("Date is not updated")


csv_to_parquet(users_dir)
csv_to_parquet(promo_dir)
csv_to_parquet(tx_dir)


# the below code are too specific to this usecase


# promo_csv = promo_dir + '/' + [file for file in os.listdir(promo_dir) if file.endswith('.csv')][0]
# tx_csv = tx_dir + '/' + [file for file in os.listdir(tx_dir) if file.endswith('.csv')][0]
# users_csv = users_dir + '/' + [file for file in os.listdir(users_dir) if file.endswith('.csv')][0]

# promo_schema = StructType([
#     StructField("userid", IntegerType(), True), \
#     StructField("vouchercode", StringType(), True), \
#     StructField("status", StringType(), True), \
#     StructField("campaignid", IntegerType(), True), \
#     StructField("time", TimestampType(), True)
# ])

# tx_schema = StructType([
#     StructField("transid", StringType(), True), \
#     StructField("transstatus", IntegerType(), True), \
#     StructField("userid", IntegerType(), True), \
#     StructField("transactiontime", TimestampType(), True), \
#     StructField("appid", IntegerType(), True) , \
#     StructField("transtype", IntegerType(), True) , \
#     StructField("amount", DoubleType(), True) , \
#     StructField("pmcid", IntegerType(), True)
# ])

# users_schema = StructType([
#     StructField("userid", IntegerType(), True), \
#     StructField("birthdate", DateType(), True), \
#     StructField("profilelevel", IntegerType(), True), \
#     StructField("genderid", IntegerType(), True), \
#     StructField("updatedtime", TimestampType(), True)
# ])

# csv_schema_files = [(promo_csv, promo_schema), (tx_csv, tx_schema), (users_csv, users_schema)]


# dfs = [spark.read.option("sep", "\t").option("header", "true").csv(file[0]) for file in csv_schema_files]


# df_promo.show()
# df_tx.show()
# df_users.show()


# df.write.mode("overwrite").parquet('/Users/YeeSC1/Documents/Final_Project/data/destination/promotions/test')


spark.stop()
