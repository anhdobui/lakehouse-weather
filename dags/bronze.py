import os
import tempfile
import json
import urllib.parse
import hashlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from utils.crawl_web import crawl_web

def get_content_hash(content: dict) -> str:
    """
    Tạo hash từ nội dung JSON.
    Args:
        content: Dữ liệu JSON cần hash.
    Returns:
        str: Hash của nội dung.
    """
    content_str = json.dumps(content, sort_keys=True)  # Chuyển dict thành chuỗi để băm
    return hashlib.sha256(content_str.encode('utf-8')).hexdigest()

def get_data_web(url_list=None, depth=2):
    """
    Crawl dữ liệu từ danh sách URL và lưu từng URL thành file JSON riêng theo partition ngày crawl rồi upload lên MinIO.
    Args:
        url_list (list): Danh sách các URL cần crawl.
        depth (int): Độ sâu quét tối đa.
    """
    if url_list is None:
        url_list = ['https://kttv.gov.vn/Kttv/vi-VN/1/ban-tin-du-bao-canh-bao-thuy-van-thoi-han-ngan-post48585.html']

    # Kết nối MinIO qua Boto3
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000', 
        aws_access_key_id='Md2VCMy52w0sSHFiT4Sc',  
        aws_secret_access_key='tGXmXyc5RwxqO07VdInV75VCdjMQwc752s7DTXSF',  
    )

    # Tên bucket
    bucket_name = "bronze"

    # Kiểm tra bucket tồn tại
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"Bucket {bucket_name} does not exist. Creating bucket...")
        s3_client.create_bucket(Bucket=bucket_name)

    # Lấy ngày hiện tại để làm partition
    current_date = datetime.now().strftime('%Y-%m-%d')

    visited_hashes = set()  # Tập hợp hash để kiểm tra trùng lặp
    failed_urls = []  # Danh sách các URL không crawl được

    # Xử lý từng URL
    for url in url_list:
        print(f"Crawling URL: {url}")
        try:
            # Crawl dữ liệu từ URL
            data = crawl_web(url, depth=depth)

            # Tính hash và kiểm tra trùng lặp
            data_hash = get_content_hash(data)
            if data_hash in visited_hashes:
                print(f"Duplicate content detected for URL: {url}, skipping.")
                continue
            visited_hashes.add(data_hash)

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
                try:
                    s3_client.upload_file(temp_file_path, bucket_name, object_name)
                    print(f"File uploaded to MinIO at {bucket_name}/{object_name}")
                except (NoCredentialsError, PartialCredentialsError) as cred_error:
                    print(f"Credential error: {cred_error}")
                except Exception as e:
                    print(f"Failed to upload file to MinIO: {e}")
        
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
            failed_urls.append(url)

    # Log các URL không xử lý được
    if failed_urls:
        print(f"Failed to process the following URLs: {failed_urls}")

def get_data_weather():
    print("weather")

# Airflow DAG Configuration
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
    description='DAG for web crawling and storing data in MinIO Bronze layer',
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    
    # Task để crawl dữ liệu web và lưu vào MinIO
    task_crawl_web = PythonOperator(
        task_id='task_crawl_web',
        python_callable=get_data_web,
        op_kwargs={'url_list': ['https://kttv.gov.vn/Kttv/vi-VN/1/ban-tin-du-bao-canh-bao-thuy-van-thoi-han-ngan-post48585.html'], 'depth': 2}
    )
    
    # Task để xử lý dữ liệu thời tiết
    task_get_data_weather = PythonOperator(
        task_id='task_get_data_weather',
        python_callable=get_data_weather
    )
    
    task_crawl_web >> task_get_data_weather
