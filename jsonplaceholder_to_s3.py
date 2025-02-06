from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import boto3
import io
from botocore.exceptions import NoCredentialsError

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jsonplaceholder_to_s3',
    default_args=default_args,
    description='ETL para dados do JSONPlaceholder para S3',
    schedule_interval='@daily',
    tags=['etl', 's3'],
)

def extract(**kwargs):
    url = "https://jsonplaceholder.typicode.com/posts"
    resposta = requests.get(url)
    
    if resposta.status_code != 200:
        raise Exception(f"Erro na API: {resposta.status_code}")
    
    data = resposta.json()
    return data

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')
    
    df = pd.DataFrame(data)
    df_filtered = df[['id', 'title', 'userId']]
    df_filtered['title_length'] = df_filtered['title'].apply(len)
    
    return df_filtered.to_dict('records')

def load_json(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_task')
    df = pd.DataFrame(data)
    
    s3 = boto3.client('s3', region_name='us-east-2')
    bucket_name = 'pbucketaws'
    file_name = 'posts_data.json'
    
    try:
        json_data = df.to_json(orient='records')
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data)
        print(f"JSON salvo em s3://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"Erro ao salvar JSON: {str(e)}")
        raise

def load_parquet(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_task')
    df = pd.DataFrame(data)
    
    s3 = boto3.client('s3', region_name='us-east-2')
    bucket_name = 'pbucketaws'
    file_name = 'posts_data.parquet'
    
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())
        print(f"Parquet salvo em s3://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"Erro ao salvar Parquet: {str(e)}")
        raise

# Definindo as tarefas
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_json_task = PythonOperator(
    task_id='load_json_task',
    python_callable=load_json,
    provide_context=True,
    dag=dag,
)

load_parquet_task = PythonOperator(
    task_id='load_parquet_task',
    python_callable=load_parquet,
    provide_context=True,
    dag=dag,
)

# Orquestração do fluxo
extract_task >> transform_task >> [load_json_task, load_parquet_task]