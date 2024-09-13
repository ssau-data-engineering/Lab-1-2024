from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime
import pandas as pd
from elasticsearch import Elasticsearch
import os

dag_path = os.getcwd()

def read_split_file(ti):
    folder_path = f"{dag_path}/data/unparsed"

    print(folder_path)

    files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    combined_df = pd.DataFrame()

    for file in files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    print(combined_df.head())

    combined_df.to_csv(f"{dag_path}/data/final_data/combined.csv", index=False)

def filter_file(ti):
    df = pd.read_csv(f"{dag_path}/data/final_data/combined.csv")
    print(df.head())
    filtered_df = df.dropna(subset=['designation', 'region_1'])
    filtered_df.to_csv(f"{dag_path}/data/final_data/filtered.csv", index=False)

def replace_null_price(ti):
    df = pd.read_csv(f"{dag_path}/data/final_data/filtered.csv")
    df['price'].fillna(0.0, inplace=True)
    df.to_csv(f"{dag_path}/data/final_data/replaced.csv", index=False)

def save_to_csv(ti):
    df = pd.read_csv(f"{dag_path}/data/final_data/replaced.csv")
    df.to_csv(f"{dag_path}/data/final_data/final.csv", index=False)

def save_to_elasticsearch(ti):
    df = pd.read_csv(f"{dag_path}/data/final_data/replaced.csv")
    es = Elasticsearch([{'host': 'elasticsearch-kibana', 'port': 9200}])
    for _, row in df.iterrows():
        es.index(index='lab1', body=row.to_json())

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

lab1_dag = DAG('LAB1_DAG', default_args=default_args, schedule_interval='@daily')

task_1 = PythonOperator(
    task_id='read_split_file',
    python_callable=read_split_file,
    dag=lab1_dag,
)

task_2 = PythonOperator(
    task_id='filter_file',
    python_callable=filter_file,
    dag=lab1_dag,
)

task_3 = PythonOperator(
    task_id='replace_null_price',
    python_callable=replace_null_price,
    dag=lab1_dag,
)

task_4 = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    dag=lab1_dag,
)

task_5 = PythonOperator(
    task_id='save_to_elasticsearch',
    python_callable=save_to_elasticsearch,
    dag=lab1_dag,
)

task_1 >> task_2 >> task_3 >> [task_4, task_5]
