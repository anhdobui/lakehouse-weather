
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.chunks_data import chunk_data



def prepare_data_chunks():
    chunk_data(prefix_path='crawl_web')
            
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_dag',
    default_args=default_args,
    description='',
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    
    # Task để đẩy file lên MinIO
    task_prepare_data_chunks = PythonOperator(
        task_id='task_prepare_data_chunks',
        python_callable=prepare_data_chunks
    )
    
    task_prepare_data_chunks
    
