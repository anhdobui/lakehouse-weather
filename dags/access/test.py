import boto3
import os
import polars as pl
from access.minio_connection import minio_manager

# Tên bucket và key của file Parquet trên MinIO
bucket_name = "silver"
key = "processed_data/2024-12-16/weather_data.parquet"
local_file = "/tmp/temp.parquet"


df = minio_manager.download_parquet(bucket_name, key, local_file)

print("Hiển thị từng hàng:")
for row in df.iter_rows(named=True):
    print(f"Row content: {row['content']}")

# Lọc các hàng chứa từ "cleaned"
filtered_df = df.filter(df['content'].str.contains("cleaned"))
print("\nCác hàng chứa từ 'cleaned':")
print(filtered_df)
