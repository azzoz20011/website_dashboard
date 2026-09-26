FROM apache/airflow:3.3.1

USER airflow

RUN pip install --no-cache-dir \
    "pyiceberg[sql-postgres,pyarrow,glue]" \
    boto3 \
    pandas