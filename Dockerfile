FROM apache/airflow:3.3.1

USER airflow

RUN python -m pip install --no-cache-dir \
    "pyiceberg[sql-postgres,pyarrow,glue]" \
    boto3 \
    pandas \
    geoip2