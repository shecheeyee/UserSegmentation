from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom

from datetime import datetime, timedelta

import os


def save_date(ti):
    users_dates = sorted(os.listdir('/opt/airflow/data_/source/users'))
    promo_dates = sorted(os.listdir('/opt/airflow/data_/source/promotions'))
    tx_dates = sorted(os.listdir('/opt/airflow/data_/source/transactions'))
    dates = [users_dates, promo_dates, tx_dates]
    users_start, promo_start, tx_start = users_dates[1], promo_dates[1], tx_dates[1]
    start_dates = [users_start, promo_start, tx_start]

    with open('/opt/airflow/data_/date.txt', 'r') as file_1:
        lines = file_1.readlines()

        result_dates = []
        result_str = ""
        # no dates initially, assume to read from the earliest date
        if not lines:
            with open('/opt/airflow/data_/date.txt', 'w') as w:

                for date in start_dates:
                    # date = datetime.strptime(date, "%Y-%m-%d")
                    # str_date = date.strftime("%Y-%m-%d")
                    print(date)
                    result_str = result_str + date + "/0" + " "
                    result_dates.append(date)
                w.write(result_str[:-1] + "\n")
            return result_dates

        # else we read from where we left off, keeping in mind that
        # the next date might not just be one day after
        else:
            with open('/opt/airflow/data_/date.txt', 'a') as file_2:
                lines = lines[-1][:-1]
                curr_user_, curr_promo_, curr_tx_ = lines.split(" ")
                curr_user_idx = int(curr_user_.split("/")[1])
                curr_promo_idx = int(curr_promo_.split("/")[1])
                curr_tx_idx = int(curr_tx_.split("/")[1])

                curr_idx = [curr_user_idx, curr_promo_idx, curr_tx_idx]

                # handle the cases where:
                #  it might read of out range: meaning that no updates to data
                #  for the case of no updates, need to not push the same data in
                #  can return the string "no updates", handle the logic in
                #  transform.py
                for i in range(len(curr_idx)):
                    if curr_idx[i] < len(dates[i]) - 1:
                        next_idx = curr_idx[i] + 1
                        result_str = result_str + \
                            str(dates[i][next_idx + 1]) + \
                            "/" + str(next_idx) + " "
                        result_dates.append(str(dates[i][next_idx + 1]))
                    else:
                        result_str = result_str + \
                            "1900-01-01" + "/" + str(curr_idx[i])
                        result_dates.append("1900-01-01")
                file_2.write(result_str[:-1] + "\n")

                return result_dates


def get_date(ti):
    dt = ti.xcom_pull(task_ids="save_date")
    return dt


with DAG("daily_ingest", start_date=datetime(2022, 1, 22),
         schedule_interval='*/2 * * * *',  catchup=False) as dag:  # '*/2 * * * *',

    task_save_date = PythonOperator(
        task_id='save_date',
        python_callable=save_date,
        do_xcom_push=True
    )

    task_get_date = PythonOperator(
        task_id='get_date',
        python_callable=get_date
    )

    convert_to_parquet = SparkSubmitOperator(
        task_id='csv_to_parquet',
        application='/opt/airflow/operators/csv_to_parquet.py',
        jars='/opt/airflow/dags/connectors/postgresql-42.6.0.jar',
        application_args=["{{ti.xcom_pull(task_ids='get_date')}}"]
        # conn_id='spark_default' # Connection ID defined in Airflow for Spark cluster
    )

    transform_and_push = SparkSubmitOperator(
        task_id='transform_and_push',
        application='/opt/airflow/operators/transform.py',
        jars='/opt/airflow/dags/connectors/postgresql-42.6.0.jar',
        application_args=["{{ti.xcom_pull(task_ids='get_date')}}"]
        # conn_id='spark_default'
        # /Users/YeeSC1/Documents/materials/dags/
    )

    task_save_date >> task_get_date >> convert_to_parquet >> transform_and_push
