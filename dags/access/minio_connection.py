import os
from dotenv import load_dotenv,find_dotenv
from .minio_io_manager import MinIOManager
load_dotenv(dotenv_path=find_dotenv())


ENDPOINT_MINIO_URL = os.getenv("ENDPOINT_MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
# Tạo đối tượng MinIOManager
minio_manager = MinIOManager(endpoint_url=ENDPOINT_MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY,secure=False)


