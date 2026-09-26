from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator   

import pendulum
import pandas as pd
import re
import io


BUCKET = "my-portfolio-bucket-6762-us"
AWS_CONN_ID = "aws_default"

RAW_PREFIX = "raw/httpd/"
STAGING_PREFIX = "staging/httpd/"
BRONZE_PREFIX = "bronze/httpd/"


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


def get_s3():
    return S3Hook(
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1"
    )


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


# ==================================================
# TASK 1: Read + clean
# ==================================================

def read_and_clean_files(**context):

    s3 = get_s3()

    # Use Airflow run date
    run_date = context["data_interval_start"].in_timezone(
        "Asia/Riyadh"
    )

    date_prefix = run_date.format("YYYY/MM/DD")

    raw_date_prefix = (
        f"{RAW_PREFIX}"
        f"{date_prefix}/"
    )

    print(
        f"Reading from: "
        f"s3://{BUCKET}/{raw_date_prefix}"
    )

    keys = s3.list_keys(
        bucket_name=BUCKET,
        prefix=raw_date_prefix
    )

    if not keys:
        raise ValueError(
            f"No files found under "
            f"s3://{BUCKET}/{raw_date_prefix}"
        )

    cleaned_keys = []

    for key in keys:

        filename = key.split("/")[-1]

        if not (
            filename.startswith("access_log")
            or filename.startswith("error_log")
        ):
            continue

        print(f"Reading and cleaning: {filename}")

        text = read_s3_text(
            s3,
            key
        )

        rows = []

        # ------------------------------------------
        # Access logs
        # ------------------------------------------

        if filename.startswith("access_log"):

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
                print(
                    f"No valid rows in {filename}"
                )
                continue

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


        # ------------------------------------------
        # Error logs
        # ------------------------------------------

        elif filename.startswith("error_log"):

            for line in text.splitlines():

                match = ERROR_PATTERN.match(line)

                if not match:
                    continue

                rows.append(
                    match.groupdict()
                )

            if not rows:
                print(
                    f"No valid rows in {filename}"
                )
                continue

            df = pd.DataFrame(rows)

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                errors="coerce"
            )

            df = df.dropna(
                subset=["timestamp"]
            )

            df = df.drop_duplicates()


        # ------------------------------------------
        # Save cleaned file temporarily
        # ------------------------------------------

        staging_key = (
            f"{STAGING_PREFIX}"
            f"{date_prefix}/"
            f"{filename}.parquet"
        )

        upload_parquet(
            s3,
            df,
            staging_key
        )

        cleaned_keys.append(
            staging_key
        )

        print(
            f"{filename}: "
            f"{len(df)} cleaned rows"
        )

    if not cleaned_keys:
        raise ValueError(
            "No files were successfully cleaned"
        )

    return cleaned_keys

# ==================================================
# TASK 2: Write to bronze
# ==================================================

def write_to_bronze(ti):

    s3 = get_s3()

    staging_keys = ti.xcom_pull(
        task_ids="read_and_clean_files"
    )

    if not staging_keys:
        raise ValueError(
            "No cleaned files found"
        )

    for staging_key in staging_keys:

        filename = staging_key.split("/")[-1]

        folder_name = filename.replace(".parquet", "")

        bronze_key = (
            f"{BRONZE_PREFIX}"
            f"{folder_name}/"
            f"{filename}"
        )

        s3.copy_object(
            source_bucket_key=staging_key,
            dest_bucket_key=bronze_key,
            source_bucket_name=BUCKET,
            dest_bucket_name=BUCKET
        )

        print(
            f"Written: "
            f"s3://{BUCKET}/{bronze_key}"
        )


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


    read_clean_task = PythonOperator(
        task_id="read_and_clean_files",
        python_callable=read_and_clean_files
    )


    write_task = PythonOperator(
        task_id="write_to_bronze",
        python_callable=write_to_bronze
    )

    trigger_cleaning = TriggerDagRunOperator(
        task_id="trigger_cleaning_dag",
        trigger_dag_id="httpd_bronze_to_silver",
    )


    read_clean_task >> write_task >> trigger_cleaning