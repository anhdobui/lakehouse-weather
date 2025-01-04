from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.seed_milvus import seed_milvus_from_silver
import os
from dotenv import load_dotenv
# Load cấu hình từ .env
load_dotenv()

def process_seed_milvus():
    seed_milvus_from_silver(
        URI_link="http://milvusdb:19530",
        collection_name= os.getenv("MILVUS_COLLECTION_NAME"),
        silver_bucket='silver',
        silver_prefix='web'
    )
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_dag',
    default_args=default_args,
    description='DAG for processing Silver data into Gold layer with chunking',
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    
    task_seed_data_milvus = PythonOperator(
        task_id='task_seed_data_milvus',
        python_callable=process_seed_milvus
    )
