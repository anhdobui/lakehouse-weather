import boto3
from io import BytesIO
import json
import os
import polars as pl  # Sử dụng cho file Parquet
from botocore.exceptions import ClientError

class MinIOManager:
    def __init__(self, endpoint_url, access_key, secret_key, secure=False):
        """
        Khởi tạo kết nối MinIO sử dụng boto3.

        :param endpoint_url: URL endpoint của MinIO
        :param access_key: Access key
        :param secret_key: Secret key
        :param secure: True nếu dùng HTTPS, False nếu HTTP
        """
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            use_ssl=secure
        )
    
    def make_bucket(self, bucket_name):
        """
        Tạo bucket nếu chưa tồn tại.
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' đã được tạo.")
        except self.client.exceptions.BucketAlreadyOwnedByYou:
            print(f"Bucket '{bucket_name}' đã tồn tại.")

    def upload_json(self, bucket_name, key, data):
        """
        Ghi dữ liệu JSON lên MinIO.

        :param bucket_name: Tên bucket
        :param key: Đường dẫn lưu file trong bucket
        :param data: Dữ liệu JSON (dictionary)
        """
        # Kiểm tra và tạo bucket nếu chưa tồn tại
        try:
            self.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':  # Bucket không tồn tại
                print(f"Bucket {bucket_name} không tồn tại. Đang tạo mới...")
                self.client.create_bucket(Bucket=bucket_name)
            else:
                raise

        # Upload dữ liệu JSON
        try:
            json_data = json.dumps(data, ensure_ascii=False, indent=4)
            self.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=BytesIO(json_data.encode("utf-8")),
                ContentType="application/json"
            )
            print(f"Dữ liệu đã được ghi vào MinIO: {bucket_name}/{key}")
        except Exception as e:
            print(f"Lỗi khi ghi dữ liệu JSON vào MinIO: {e}")


    def read_json(self, bucket_name, key):
        """
        Đọc file JSON từ MinIO và trả về nội dung dưới dạng dictionary hoặc JSON object.

        :param bucket_name: Tên bucket
        :param key: Đường dẫn file trong bucket
        :return: Dữ liệu JSON (dictionary hoặc JSON object)
        """
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read()
            data = json.loads(content.decode('utf-8'))
            print(f"Dữ liệu JSON đã được đọc từ MinIO: {bucket_name}/{key}")
            return data
        except self.client.exceptions.NoSuchKey:
            print(f"File '{key}' không tồn tại trong bucket '{bucket_name}'.")
            return None
        except Exception as e:
            print(f"Lỗi khi đọc file JSON từ MinIO: {e}")
            return None

    def upload_parquet(self, bucket_name, key, dataframe):
        """
        Ghi DataFrame (Polars hoặc Pandas) lên MinIO dưới dạng file Parquet.

        :param bucket_name: Tên bucket
        :param key: Đường dẫn lưu file trong bucket
        :param dataframe: DataFrame cần ghi (Polars hoặc Pandas)
        """
        # Tạo bucket nếu chưa tồn tại
        try:
            self.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':  # Bucket không tồn tại
                print(f"Bucket {bucket_name} không tồn tại. Đang tạo mới...")
                self.client.create_bucket(Bucket=bucket_name)
            else:
                raise

        # Tạm thời lưu DataFrame thành file Parquet
        temp_file = "/tmp/temp.parquet"
        dataframe.write_parquet(temp_file)

        try:
            # Upload file Parquet lên MinIO
            self.client.upload_file(temp_file, bucket_name, key)
            print(f"File Parquet đã được ghi vào MinIO: {bucket_name}/{key}")
        finally:
            # Xóa file tạm sau khi upload
            os.remove(temp_file)
    
    def upload(self, bucket_name, key, data, content_type="application/octet-stream"):
        """
        Hàm upload linh hoạt để đẩy bất kỳ loại dữ liệu nào lên MinIO.

        :param bucket_name: Tên bucket
        :param key: Đường dẫn lưu file trong bucket
        :param data: Dữ liệu có thể là chuỗi, bytes, đường dẫn file, hoặc DataFrame
        :param content_type: Loại nội dung MIME (mặc định là application/octet-stream)
        """
        # Kiểm tra và tạo bucket nếu chưa tồn tại
        try:
            self.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':  # Bucket không tồn tại
                print(f"Bucket {bucket_name} không tồn tại. Đang tạo mới...")
                self.client.create_bucket(Bucket=bucket_name)
            else:
                raise

        # Xử lý dữ liệu đầu vào
        if isinstance(data, str):  # Chuỗi (file path hoặc text)
            if os.path.exists(data):  # Nếu là đường dẫn file
                with open(data, "rb") as file:
                    self.client.upload_fileobj(file, bucket_name, key)
            else:  # Nếu là chuỗi văn bản
                self.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data.encode("utf-8"),
                    ContentType=content_type
                )
        elif isinstance(data, bytes):  # Dữ liệu dạng bytes
            self.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=BytesIO(data),
                ContentType=content_type
            )
        elif hasattr(data, "write_parquet"):  # DataFrame (Polars hoặc Pandas)
            temp_file = "/tmp/temp.parquet"
            data.write_parquet(temp_file)
            try:
                self.client.upload_file(temp_file, bucket_name, key)
            finally:
                os.remove(temp_file)
        else:
            raise ValueError("Kiểu dữ liệu không được hỗ trợ.")

        print(f"Dữ liệu đã được upload lên MinIO: {bucket_name}/{key}")

    def download_parquet(self, bucket_name, key, local_file):
        """
        Tải file Parquet từ MinIO và trả về DataFrame (Pandas).

        :param bucket_name: Tên bucket
        :param key: Đường dẫn file trong bucket
        :param local_file: Đường dẫn lưu file cục bộ
        :return: DataFrame (Pandas)
    """
        self.client.download_file(bucket_name, key, local_file)
        df = pl.read_parquet(local_file)  # Sử dụng Pandas
        os.remove(local_file)
        return df
    def list_objects(self, bucket_name, prefix=None):
        """
        Liệt kê tất cả các object trong bucket với prefix cụ thể.

        :param bucket_name: Tên bucket
        :param prefix: Đường dẫn thư mục hoặc tiền tố để lọc object (nếu có)
        :return: Danh sách object (tên file)
        """
        try:
            response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            objects = [obj["Key"] for obj in response.get("Contents", [])]
            print(f"Đã tìm thấy {len(objects)} object trong {bucket_name} với prefix '{prefix}'.")
            return objects
        except ClientError as e:
            print(f"Lỗi khi liệt kê object trong bucket {bucket_name}: {e}")
            return []
