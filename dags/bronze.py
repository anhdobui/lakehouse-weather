import os
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.crawl_web import crawl_web
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import json
import urllib.parse

def get_data_web():
    """
    Crawl dữ liệu từ danh sách URL và lưu từng URL thành file JSON riêng theo partition ngày crawl rồi upload lên MinIO.
    """
    # Danh sách URL cần crawl
    url_list = ['https://kttv.gov.vn/Kttv/vi-VN/1/ban-tin-du-bao-canh-bao-thuy-van-thoi-han-ngan-post48585.html']

    # Kết nối MinIO qua Boto3
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',  # Thay bằng endpoint của MinIO, ví dụ: 'http://localhost:9000'
        aws_access_key_id='Md2VCMy52w0sSHFiT4Sc',        # Thay bằng access key của bạn
        aws_secret_access_key='tGXmXyc5RwxqO07VdInV75VCdjMQwc752s7DTXSF',    # Thay bằng secret key của bạn
    )

    # Tên bucket
    bucket_name = "bronze"

    # Kiểm tra bucket tồn tại
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        # Tạo bucket nếu chưa tồn tại
        s3_client.create_bucket(Bucket=bucket_name)

    # Lấy ngày hiện tại để làm partition
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Xử lý từng URL
    for url in url_list:
        print(f"Crawling URL: {url}")
        try:
            # Crawl dữ liệu từ URL
            depth = 2
            data = crawl_web(url,depth=depth)  # Hàm crawl_web xử lý từng URL

            # Tạo thư mục tạm
            with tempfile.TemporaryDirectory() as temp_dir:
                # Tách netloc và path từ URL
                parsed_url = urllib.parse.urlparse(url)
                netloc = parsed_url.netloc  # Ví dụ: www.kttv.gov.vn
                path = parsed_url.path.strip("/")  # Ví dụ: kttv

                # Ghép netloc và path, thay thế dấu "/" bằng "_"
                file_name = f"{netloc}_{path}".replace("/", "_")
                file_name = f"{os.path.splitext(file_name)[0]}.json"
                temp_file_path = os.path.join(temp_dir, file_name)

                # Lưu dữ liệu vào file JSON
                with open(temp_file_path, "w", encoding="utf-8") as file:
                    json.dump(data, file, ensure_ascii=False, indent=4)

                print(f"Temporary file created at: {temp_file_path}")

                # Đường dẫn trong MinIO với partition theo ngày
                object_name = f"crawl_web/{current_date}/{file_name}"

                # Upload file JSON lên MinIO
                s3_client.upload_file(temp_file_path, bucket_name, object_name)
                print(f"File uploaded to MinIO at {bucket_name}/{object_name}")
        
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
def get_data_weather():
    print("weather")

  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_dag',
    default_args=default_args,
    description='',
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    
    # Task để đẩy file lên MinIO
    task_crawl_web = PythonOperator(
        task_id='task_crawl_web',
        python_callable=get_data_web
    )
    
    # Task để đẩy file lên MinIO
    task_get_data_weather = PythonOperator(
        task_id='task_get_data_weather',
        python_callable=get_data_weather
    )
    
    task_crawl_web >> task_get_data_weather
    
