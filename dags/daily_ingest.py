from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom

from datetime import datetime, timedelta




def save_date(ti):
    with open('/opt/airflow/data_/date.txt', 'r') as file_1:
        lines = file_1.readlines()
        
        if not lines:
            with open('/opt/airflow/data_/date.txt', 'w') as w:
                date_1 = datetime.strptime("2021-11-01", "%Y-%m-%d") 
                str_date = date_1.strftime("%Y-%m-%d")
                w.write(str_date + "\n")
            return '2021-11-01'
        else:
            with open('/opt/airflow/data_/date.txt', 'a') as file_2:
                print("length of date")
                print(len(lines[-1][:-1]))
                print(lines[-1][:-1])
                date_2 = datetime.strptime(lines[-1][:-1], "%Y-%m-%d") + timedelta(days=1)
                str_date = date_2.strftime("%Y-%m-%d")
                file_2.write(str_date + "\n")
                return str_date



def get_date(ti):
    dt = ti.xcom_pull(task_ids="save_date")
    return dt


with DAG("daily_ingest", start_date=datetime(2022, 1, 22), 
         schedule_interval='*/5 * * * *',  catchup=False) as dag: #'*/2 * * * *',
    
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
        jars = '/opt/airflow/dags/connectors/postgresql-42.6.0.jar',
        application_args=["{{ti.xcom_pull(task_ids='get_date')}}"]
        # conn_id='spark_default' # Connection ID defined in Airflow for Spark cluster  
    )
    
    transform_and_push = SparkSubmitOperator(
        task_id='transform_and_push',
        application='/opt/airflow/operators/transform.py',
        jars = '/opt/airflow/dags/connectors/postgresql-42.6.0.jar',
        application_args=["{{ti.xcom_pull(task_ids='get_date')}}"]
        # conn_id='spark_default'
        #/Users/YeeSC1/Documents/materials/dags/
    )

    task_save_date >> task_get_date >> convert_to_parquet >> transform_and_push