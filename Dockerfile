FROM anhdobui/airflow-custom:v1

# Copy requirements file
COPY requirements.txt .

# Thiết lập PYTHONPATH
ENV PYTHONPATH=/opt/airflow/dags

# Chuyển sang quyền root để thực thi các lệnh yêu cầu
USER root

# Xóa các gói bị lỗi
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/tokenizers*

# Quay lại quyền airflow để cài đặt gói
USER airflow

# Cài đặt các gói
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir setuptools wheel && \
    python3 -m pip install --no-cache-dir -r requirements.txt --default-timeout=100
