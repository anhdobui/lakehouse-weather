import os
from uuid import uuid4
from langchain_openai import OpenAIEmbeddings
from langchain_milvus import Milvus
from langchain.schema import Document
from access.minio_connection import minio_manager  # Kết nối MinIO từ file minio_connection.py
def connect_to_milvus(URI_link: str, collection_name: str) -> Milvus:
    """
    Hàm kết nối đến collection có sẵn trong Milvus
    Args:
        URI_link (str): Đường dẫn kết nối đến Milvus
        collection_name (str): Tên collection cần kết nối
    Returns:
        Milvus: Đối tượng Milvus đã được kết nối, sẵn sàng để truy vấn
    Chú ý:
        - Không tạo collection mới hoặc xóa dữ liệu cũ
        - Sử dụng model 'text-embedding-3-large' cho việc tạo embeddings khi truy vấn
    """
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    vectorstore = Milvus(
        embedding_function=embeddings,
        connection_args={"uri": URI_link},
        collection_name=collection_name,
    )
    return vectorstore
def split_documents(documents, batch_size=100):
    """
    Chia danh sách documents thành các batch nhỏ hơn.
    """
    for i in range(0, len(documents), batch_size):
        yield documents[i:i + batch_size]
def seed_milvus_from_silver(URI_link: str, collection_name: str, silver_bucket: str, silver_prefix: str) -> Milvus:
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

    object_keys = minio_manager.list_objects(silver_bucket, silver_prefix)
    if not object_keys:
        print(f"Không tìm thấy dữ liệu trong bucket '{silver_bucket}' với prefix '{silver_prefix}'")
        return

    print(f"Đã tìm thấy {len(object_keys)} file trong bucket '{silver_bucket}' với prefix '{silver_prefix}'")

    documents = []
    for object_key in object_keys:
        print(f"Đang xử lý file: {object_key}")
        raw_data = minio_manager.read_json(silver_bucket, object_key)
        if not raw_data:
            print(f"Không có dữ liệu trong file {object_key}")
            continue

        for doc in raw_data:
            try:
                document = Document(
                    page_content=doc.get("split_content", ""),
                    metadata={
                        **doc.get("metadata", {}),
                        "title": doc.get("title", "Untitled"),  # Thêm giá trị mặc định cho title
                        "description": doc.get("description", "No description available"),  # Giá trị mặc định cho description
                        "content_type": doc.get("content_type", "text/plain")  # Giá trị mặc định cho content_type
                    }
                )
                documents.append(document)
            except Exception as e:
                print(f"Lỗi khi tạo Document từ file {object_key}: {e}")

    if not documents:
        print("Không có Document nào được tạo từ tầng Silver.")
        return

    print(f"Đã tạo {len(documents)} Document từ dữ liệu tầng Silver.")

    vectorstore = Milvus(
        embedding_function=embeddings,
        connection_args={"uri": URI_link},
        collection_name=collection_name,
        drop_old=True
    )

    batch_size = 100
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        uuids = [str(uuid4()) for _ in range(len(batch))]
        try:
            vectorstore.add_documents(documents=batch, ids=uuids)
            print(f"Đã lưu {len(batch)} vector vào collection '{collection_name}' trong Milvus.")
        except Exception as e:
            print(f"Lỗi khi lưu batch vào Milvus: {e}")

    return vectorstore