from datetime import timedelta
import airflow
import pandas as pd
import numpy as np
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# a bit of time travel magic, get date for today..
dag_path = os.getcwd()
date_file = open("/opt/airflow/raw_data/current_day.txt", "r")
today = date_file.readline().strip()



## Our DAG
user_data_dag = DAG(
    'user_data_dag_tt_word',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
    },
    schedule_interval = timedelta(days=1),
    catchup=False
    )

def start_import():
    print("starting import")

## Our Task
start_import_task = PythonOperator(
    task_id='start_import',
    python_callable=start_import,
    dag=user_data_dag,
)

def load_users():
    user_data = pd.read_csv(F"/opt/airflow/raw_data/users_{today}.csv")
    user_data.head()
    user_data.fillna({
        'name': 'not known', 
        'status': 'standard'
    }, inplace=True)
    user_data.to_csv(f"/opt/airflow/imported_data/{today}/users.csv", index=False)

## Our Task
load_users_task = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=user_data_dag,
)

def load_orders():
    # import new orders
    order_data = pd.read_csv(f"/opt/airflow/raw_data/orders_{today}.csv")

    import os  
    # safe import 
    # mkdir if not exist
    os.makedirs(f"/opt/airflow/imported_data/{today}/", exist_ok=True)  
    order_data.to_csv(f"/opt/airflow/imported_data/{today}/orders.csv", index=False)

    # join to old order data to create a complete "view"
    already_imported_order_data = pd.read_csv("/opt/airflow/processed_data/orders.csv")

    result = pd.concat([already_imported_order_data, order_data], ignore_index=True)
    
    # safe result as view and as timestamped view (in real life, just make your VIEW walk through the dirs.)
    result.to_csv(f"/opt/airflow/processed_data/orders.csv", index=False)
    
    os.makedirs(f"/opt/airflow/processed_data/{today}/", exist_ok=True)  
    result.to_csv(f"/opt/airflow/processed_data/{today}/orders.csv", index=False)

# # Our Task
load_orders_task = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=user_data_dag,
)

def process_users_orders():
    user_data = pd.read_csv(f"/opt/airflow/imported_data/{today}/users.csv")
    order_data = pd.read_csv(f"/opt/airflow/processed_data/{today}/orders.csv")

    result = order_data.merge(user_data, on="user_id", how="left") #

    #mkdir if not exist
    import os  
    os.makedirs(f"/opt/airflow/processed_data/{today}/", exist_ok=True)  

    result.groupby(["sales_date","status"]).sum().reset_index().to_csv(f"/opt/airflow/processed_data/{today}/agg_sales.csv")
# # Our Task

process_users_orders_task = PythonOperator(
    task_id='process_users_orders',
    python_callable=process_users_orders,
    dag=user_data_dag,
)

from airflow.models.baseoperator import chain

chain(start_import_task,[load_orders_task, load_users_task],process_users_orders_task)