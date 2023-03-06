from datetime import timedelta
import airflow
import pandas as pd
import numpy as np
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

dag_path = os.getcwd()

def load_users():
    user_data = pd.read_csv("/opt/airflow/raw_data/users_day_1.csv")
    user_data.head()
    user_data.fillna({
        'name': 'not known', 
        'status': 'standard'
    }, inplace=True)
    user_data.to_csv("processed_data/imported_data/user_data.csv", index=False)

def load_orders():
    # import new orders
    order_data = pd.read_csv("/opt/airflow/raw_data/orders_day_1.csv")
    order_data.head()
    order_data.fillna({
        'name': 'not known', 
        'status': 'standard'
    }, inplace=True)

    # join to old order data
    already_imported_order_data = pd.read_csv("/opt/airflow/imported_data/orders.csv")

    result = pd.concat([already_imported_order_data, order_data], ignore_index=True)
    
    # safe result as csv
    result.to_csv("processed_data/imported_data/orders.csv", index=False)

def process_users_orders():
    user_data = pd.read_csv("/opt/airflow/raw_data/users_day_1.csv")
    order_data = pd.read_csv("/opt/airflow/imported_data/orders.csv")



def load_users_message():
    print("done")

## Our DAG
user_data_dag = DAG(
    'user_data_dag',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
    },
    schedule_interval = timedelta(days=1),
    catchup=False
    )

## Our Tasks
load_data = PythonOperator(
    task_id='loading_data',
    python_callable=load_users,
    dag=user_data_dag,
)

message = PythonOperator(
    task_id='message_data',
    python_callable=load_users_message,
    dag=user_data_dag,
)

load_data >> message