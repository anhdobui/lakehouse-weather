FROM anhdobui/airflow-custom:v1


COPY requirements.txt .

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt --default-timeout=100

