from pyspark.sql import SparkSession
from pyspark.sql import functions as func

import sys

# Create a Spark session
spark = SparkSession.builder \
    .appName("DailyTransform") \
    .master("local") \
    .config("spark.driver.extraClassPath", "/opt/airflow/dags/connectors/postgresql-42.6.0.jar") \
    .getOrCreate()

# Define psql connection properties
psql_properties = {
    "user": "airflow",
    "password": "airflow",
    "url": "jdbc:postgresql://postgres:5432/customers",
    "driver": "org.postgresql.Driver"  # "com.psql.cj.jdbc.Driver"
}

args = sys.argv[1:]

date = args[0]
date = date[1:-1].split(", ")


user_date = date[0][1:-1]
promo_date = date[1][1:-1]
tx_date = date[2][1:-1]

dst_path = '/opt/airflow/data_/destination'

# transform some columns
# eg map the genders, don't want to maintain another table just for gender which is binary


def loadGenderMapping():
    gender_csv = spark.sparkContext.textFile(
        '/opt/airflow/data_/source/mapping/gender.csv')
    csv_cols_rdd = gender_csv.map(lambda x: x.split('\t')).collect()
    genderMap = {}

    for k, v in csv_cols_rdd[1:]:
        genderMap[int(k)] = v
    return genderMap


# broadcast object that gets the data from another table
nameDict = spark.sparkContext.broadcast(loadGenderMapping())

# creating a user defined funciton to look up gender from our broadcast object


def lookUpGender(gender):
    return nameDict.value[int(gender)]


lookupGenderUDF = func.udf(lookUpGender)

# load to dataframe


try:
    df_users = spark.read.parquet(dst_path + '/users/' + promo_date)
    # adding a column
    df_users.show()
    df_users = df_users.withColumn(
        "gender_", lookupGenderUDF(func.col("gender")))
    df_users = df_users.drop(func.col("gender"))
    df_users = df_users.withColumnRenamed("gender_", "gender")

    # check for duplicates
    print("--------duplicated userid--------")
    df_users.groupby("userid") \
        .count() \
        .where("count > 1") \
        .drop("count").show()
    df_users.count()
    print("---------------------------------")

    # due to some duplicates being present in the data:
    # group by userid and get the newest data
    latest_updated_users = df_users.groupBy("userid").agg(
        func.max("updatedtime").alias('updatedtime'))
    df_users = df_users.join(latest_updated_users, on=[
                             'userid', 'updatedtime'], how="right")
    # df_users.show()

    print("--------duplicated userid--------")
    df_users.groupby("userid") \
        .count() \
        .where("count > 1") \
        .drop("count").show()
    print("---------------------------------")
except FileNotFoundError:
    print("Date not updated")
    users_schema = spark.read\
        .format("jdbc")\
        .option("driver", psql_properties["driver"])\
        .option("url", psql_properties["url"])\
        .option("user", psql_properties["user"])\
        .option("password", psql_properties["password"])\
        .option("dbtable", "usrers")\
        .load().schema
    df_tx = spark.createDataFram([], schema=users_schema)


try:
    df_promo = spark.read.parquet(dst_path + '/promotions/' + user_date)
except FileNotFoundError:
    print("Date not updated")
    promo_schema = spark.read\
        .format("jdbc")\
        .option("driver", psql_properties["driver"])\
        .option("url", psql_properties["url"])\
        .option("user", psql_properties["user"])\
        .option("password", psql_properties["password"])\
        .option("dbtable", "promotions")\
        .load().schema
    df_promo = spark.createDataFrame([], schema=promo_schema)


try:
    df_tx = spark.read.parquet(dst_path + '/transactions/' + tx_date)
except FileNotFoundError:
    print("Date not updated")
    tx_schema = spark.read\
        .format("jdbc")\
        .option("driver", psql_properties["driver"])\
        .option("url", psql_properties["url"])\
        .option("user", psql_properties["user"])\
        .option("password", psql_properties["password"])\
        .option("dbtable", "transactions")\
        .load().schema
    df_tx = spark.createDataFram([], schema=tx_schema)


# # look up appid, some appids in tx are not in the mapping table
# # but this reduces the number of tx from 3k to 90

# df_appid = spark.read\
#     .format("jdbc")\
#     .option("driver", psql_properties["driver"])\
#     .option("url", psql_properties["url"])\
#     .option("user", psql_properties["user"])\
#     .option("password", psql_properties["password"])\
#     .option("dbtable", "merchants")\
#     .load()

# app_ids = df_appid.select(func.col("appid").cast('int').alias("appid"))

# app_ids.show()
# df_tx = df_tx.join(app_ids, on=['appid'], how="right")


# stores the dataframe and the name of table in psql db
dfs = [(df_users, 'users'), (df_promo, 'promotions'),  (df_tx, 'transactions')]


def write_to_db(df, name):
    print("\n######################################################################")
    print(f"Writing to {name}...")
    # Write the DataFrame to psql

    df.write.jdbc(
        url=psql_properties["url"],
        table=name,
        mode="append",
        properties={
            **psql_properties,
            # Adjust the timeout value as needed
            "connectionProperties": "connectTimeout=600"
        }
    )


for df in dfs:
    write_to_db(df[0], df[1])


spark.stop()
