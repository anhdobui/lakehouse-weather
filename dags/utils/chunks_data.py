import re
import os
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from access.minio_connection import minio_manager  # Từ file minio_connection.py

def sanitize_object_name(object_name, prefix_path):
    """
    Loại bỏ prefix từ object_name và giữ lại chỉ tên tệp,
    sau đó thêm '_chunked' trước phần mở rộng tệp.
    """
    # Loại bỏ prefix để lấy phần còn lại
    if object_name.startswith(prefix_path):
        file_name_with_extension = object_name[len(prefix_path):]
    else:
        file_name_with_extension = object_name

    # Lấy tên file từ đường dẫn (loại bỏ thư mục nếu có)
    file_name_with_extension = os.path.basename(file_name_with_extension)

    # Tách phần tên file và phần mở rộng
    file_name, file_extension = os.path.splitext(file_name_with_extension)

    # Tạo tên file mới với '_chunked' trước phần mở rộng
    sanitized_name = f"{file_name}_chunked{file_extension}"
    return sanitized_name


def delete_existing_data(bucket_name, prefix):
    """
    Xóa toàn bộ dữ liệu cũ trong một bucket và prefix cụ thể.

    :param bucket_name: Tên bucket trong MinIO.
    :param prefix: Đường dẫn prefix để xóa các object bên trong.
    """
    object_keys = minio_manager.list_objects(bucket_name, prefix)
    if object_keys:
        for key in object_keys:
            try:
                minio_manager.client.delete_object(Bucket=bucket_name, Key=key)
                print(f"Đã xóa object: {key}")
            except Exception as e:
                print(f"Lỗi khi xóa object {key}: {e}")
    else:
        print(f"Không có dữ liệu nào trong prefix: {prefix} để xóa.")

def chunk_data(prefix_path):
    """
    Xử lý tất cả các object JSON từ MinIO trong một prefix path cụ thể và tách thành các chunk.

    :param prefix_path: Đường dẫn prefix để quét các object trong MinIO.
    """
    bronze_bucket = "bronze"
    silver_bucket = "silver"
    silver_prefix = "web"
    
    # Xóa dữ liệu cũ trong tầng Silver
    print(f"Đang xóa dữ liệu cũ trong bucket '{silver_bucket}' tại prefix '{silver_prefix}'...")
    delete_existing_data(silver_bucket, silver_prefix)

    object_keys = minio_manager.list_objects(bronze_bucket, prefix_path)
    if not object_keys:
        print(f"Không có object nào trong path: {prefix_path}")
        return

    print(f"Đã tìm thấy {len(object_keys)} object trong path: {prefix_path}")

    # Xử lý từng object trong bucket Bronze
    for object_key in object_keys:
        print(f"Đang xử lý object: {object_key}")
        raw_data = minio_manager.read_json(bronze_bucket, object_key)
        
        if not raw_data:
            print(f"Không có dữ liệu trong object: {object_key}")
            continue

        print(f"Đã đọc {len(raw_data)} mục dữ liệu từ MinIO object: {object_key}")

        # Tạo các đối tượng Document từ dữ liệu
        docs = []
        for idx, content in enumerate(raw_data):
            if 'page_content' in content:
                try:
                    doc = Document(
                        page_content=content['page_content'],
                        metadata=content.get("metadata", {})
                    )
                    docs.append(doc)
                except Exception as e:
                    print(f"Lỗi khi tạo Document từ mục dữ liệu thứ {idx + 1}: {e}")
            else:
                print(f"Mục dữ liệu thứ {idx + 1} không có trường 'page_content'.")

        if not docs:
            print(f"Không có Document nào được tạo từ object: {object_key}")
            continue

        print(f"Đã tạo {len(docs)} Document từ dữ liệu trong object: {object_key}")

        # Sử dụng RecursiveCharacterTextSplitter để chia nhỏ các Document
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=5000, chunk_overlap=500)
        all_splits = text_splitter.split_documents(docs)

        print(f"Đã tách thành {len(all_splits)} đoạn văn bản sau khi chia từ object: {object_key}")

        # Chuyển các đoạn văn bản thành định dạng JSON để lưu vào MinIO
        chunked_content = [
            {"split_content": split.page_content, "metadata": split.metadata}
            for split in all_splits
        ]

        if not chunked_content:
            print(f"Không có dữ liệu để lưu từ object: {object_key}")
            continue

        sanitized_key = sanitize_object_name(object_key, prefix_path)
        json_key = os.path.join(silver_prefix, sanitized_key)  

        # Lưu dữ liệu thành JSON và upload lên MinIO tầng Silver
        try:
            minio_manager.upload_json(bucket_name=silver_bucket, key=json_key, data=chunked_content)
            print(f"Dữ liệu đã được tách thành chunk và lưu tại: {silver_bucket}/{json_key}")
        except Exception as e:
            print(f"Lỗi khi upload dữ liệu lên MinIO từ object {object_key}: {e}")
