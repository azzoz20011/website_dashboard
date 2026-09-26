from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pendulum
import pandas as pd
import re
import io


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET = "my-portfolio-bucket-6762"
AWS_CONN_ID = "aws_default"

RAW_PREFIX = "raw/httpd/"
BRONZE_PREFIX = "bronze/httpd/"


# --------------------------------------------------
# Regex patterns
# --------------------------------------------------

ACCESS_PATTERN = re.compile(
    r'(?P<ip_address>\S+) '
    r'\S+ \S+ '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) '
    r'(?P<path>\S+) '
    r'(?P<protocol>[^"]+)" '
    r'(?P<status_code>\d{3}) '
    r'(?P<response_size>\S+)'
)


ERROR_PATTERN = re.compile(
    r'\[(?P<timestamp>[^\]]+)\] '
    r'\[(?P<module>[^\]]+)\] '
    r'\[(?P<pid>[^\]]+)\] '
    r'(?P<message>.*)'
)
# --------------------------------------------------
# Read S3 file
# --------------------------------------------------


def read_s3_text(s3, key):

    obj = s3.get_key(
        key=key,
        bucket_name=BUCKET
    )

    return (
        obj.get()["Body"]
        .read()
        .decode("utf-8", errors="ignore")
    )


# --------------------------------------------------
# Upload DataFrame as Parquet
# --------------------------------------------------

def upload_parquet(s3, df, key):

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False
    )

    buffer.seek(0)

    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=key,
        bucket_name=BUCKET,
        replace=True
    )

    print(f"Uploaded: s3://{BUCKET}/{key}")


# --------------------------------------------------
# Process access log
# --------------------------------------------------

def process_access_file(s3, key):

    print(f"Processing access log: {key}")

    text = read_s3_text(s3, key)

    rows = []

    for line in text.splitlines():

        match = ACCESS_PATTERN.match(line)

        if not match:
            continue

        row = match.groupdict()

        row["status_code"] = int(
            row["status_code"]
        )

        row["response_size"] = (
            int(row["response_size"])
            if row["response_size"].isdigit()
            else 0
        )

        rows.append(row)

    if not rows:
        print(f"No valid rows found in {key}")
        return

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%d/%b/%Y:%H:%M:%S %z",
        errors="coerce"
    )

    df = df.dropna(
        subset=["timestamp"]
    )

    df = df.drop_duplicates()

    filename = key.split("/")[-1]

    bronze_key = (
        f"{BRONZE_PREFIX}"
        f"{filename}.parquet"
    )

    upload_parquet(
        s3,
        df,
        bronze_key
    )

    print(
        f"{filename}: "
        f"{len(df)} rows written"
    )


# --------------------------------------------------
# Process error log
# --------------------------------------------------

def process_error_file(s3, key):

    print(f"Processing error log: {key}")

    text = read_s3_text(s3, key)

    rows = []

    for line in text.splitlines():

        match = ERROR_PATTERN.match(line)

        if not match:
            continue

        row = match.groupdict()

        rows.append(row)

    if not rows:
        print(f"No valid rows found in {key}")
        return

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    df = df.drop_duplicates()

    filename = key.split("/")[-1]

    bronze_key = (
        f"{BRONZE_PREFIX}"
        f"{filename}.parquet"
    )

    upload_parquet(
        s3,
        df,
        bronze_key
    )

    print(
        f"{filename}: "
        f"{len(df)} rows written"
    )


# --------------------------------------------------
# Main Airflow task
# --------------------------------------------------

def raw_to_bronze():

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1"
    )

    keys = s3.list_keys(
        bucket_name=BUCKET,
        prefix=RAW_PREFIX
    )

    if not keys:
        raise ValueError(
            f"No files found under "
            f"s3://{BUCKET}/{RAW_PREFIX}"
        )

    for key in keys:

        filename = key.split("/")[-1]

        if filename.startswith("access_log"):

            process_access_file(
                s3,
                key
            )

        elif filename.startswith("error_log"):

            process_error_file(
                s3,
                key
            )

        else:

            print(
                f"Skipping unknown file: "
                f"{filename}"
            )


# --------------------------------------------------
# DAG
# --------------------------------------------------

with DAG(

    dag_id="httpd_raw_to_bronze",

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Riyadh"
    ),

    schedule=None,

    catchup=False,

    tags=[
        "httpd",
        "s3",
        "bronze"
    ],

) as dag:

    raw_to_bronze_task = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=raw_to_bronze
    )