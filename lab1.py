import os

import pandas as pd

from elasticsearch import Elasticsearch

import datetime

import pendulum

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="lab1_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 9, 23, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["labs", "pandas"],
) as dag:
    
    @task(task_id='read_files')
    def read_df():
        filename_tmpl = 'chunk'
        min_idx, max_idx = 0, 25
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path_to_input = os.path.join(current_dir, '..', 'data', 'input')
        dfs = []
        for i in range(min_idx, max_idx + 1):
            df = pd.read_csv(os.path.join(path_to_input, f'{filename_tmpl}{i}.csv'))
            dfs.append(df)
        df = pd.concat(dfs, axis=0)
        df.to_csv('buffer.csv', index=False)

    @task(task_id='dropna')
    def drop_nans():
        df = pd.read_csv('buffer.csv')
        df.dropna(subset=['designation', 'region_1'], inplace=True, axis=0)
        df.to_csv('buffer.csv', index=False)

    @task(task_id='fillna')
    def fill_nans():
        df = pd.read_csv('buffer.csv')
        df.fillna({'price': 0.})
        df.to_csv('buffer.csv', index=False)

    @task(task_id='save_to_out')
    def save_res():
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path_to_output = os.path.join(current_dir, '..', 'data')
        df = pd.read_csv('buffer.csv')
        df.to_csv(os.path.join(path_to_output, 'output.csv'), index=False)


    @task(task_id='save_to_elasticsearch')
    def save_to_els():
        df = pd.read_csv('buffer.csv')
        es = Elasticsearch("http://elasticsearch-kibana:9200")
        for index, row in df.iterrows():
            es.index(index='dag1_output', body=row.to_json())


    read_task, drop_task, fill_task, to_csv_task, to_es_task = read_df(), drop_nans(), fill_nans(), save_res(), save_to_els()

    read_task >> drop_task >> fill_task >> [to_csv_task, to_es_task]
    
