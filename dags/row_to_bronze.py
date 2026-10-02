from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator   
import time
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

import pendulum
import pandas as pd
import re
import io

REGION = "us-east-1"

BUCKET = "my-portfolio-bucket-6762-us"
AWS_CONN_ID = "aws_default"

RAW_PREFIX = "raw/httpd/"
STAGING_PREFIX = "staging/httpd/"
LOCATION_PREFIX = "staging/location/"
DISTINCT_PREFIX = "staging/distinct/"

BRONZE_PREFIX = "bronze/httpd/"
DATABASE = "stedi"
ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/"
)
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
def get_athena_client():

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="athena",
        region_name=REGION
    )

    return hook.get_client_type()


# --------------------------------------------------
# Run Athena Query
# --------------------------------------------------

def run_query(query):

    athena = get_athena_client()

    response = athena.start_query_execution(

        QueryString=query,

        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT
        }

    )

    query_id = response[
        "QueryExecutionId"
    ]

    print(
        f"Athena query started: "
        f"{query_id}"
    )

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        state = response[
            "QueryExecution"
        ][
            "Status"
        ][
            "State"
        ]

        if state == "SUCCEEDED":

            print("Query succeeded")

            return

        elif state in [
            "FAILED",
            "CANCELLED"
        ]:

            reason = response[
                "QueryExecution"
            ][
                "Status"
            ].get(
                "StateChangeReason",
                "Unknown error"
            )

            raise Exception(
                f"Athena query failed: "
                f"{reason}"
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

        if filename.startswith("access_log"):
            staging_key = (
                f"{STAGING_PREFIX}"
                f"{date_prefix}/access_log/"
                f"{filename}.parquet"
            )

        else:
            staging_key = (
                f"{STAGING_PREFIX}"
                f"{date_prefix}/error_log/"
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
# TASK 2: Write to bronze Iceberg tables
# ==================================================

def write_to_bronze(**context):

    run_date = context["data_interval_start"].in_timezone(
        "Asia/Riyadh"
    )

    date_prefix = run_date.format("YYYY/MM/DD")

    access_staging_path = (
        f"s3://{BUCKET}/"
        f"{STAGING_PREFIX}"
        f"{date_prefix}/access_log/"
    )

    error_staging_path = (
        f"s3://{BUCKET}/"
        f"{STAGING_PREFIX}"
        f"{date_prefix}/error_log/"
    )

    access_iceberg_path = (
        f"s3://{BUCKET}/"
        f"{BRONZE_PREFIX}"
        f"access_log/"
    )

    error_iceberg_path = (
        f"s3://{BUCKET}/"
        f"{BRONZE_PREFIX}"
        f"error_log/"
    )

    # --------------------------------------------------
    # Drop old tables
    # --------------------------------------------------

    run_query("""
        DROP TABLE IF EXISTS stedi.bronze_access_log
    """)

    run_query("""
        DROP TABLE IF EXISTS stedi.bronze_error_log
    """)

    run_query("""
        DROP TABLE IF EXISTS stedi.bronze_locations_log
    """)

    # --------------------------------------------------
    # Create temporary external tables over Parquet
    # --------------------------------------------------

    run_query(f"""
        CREATE EXTERNAL TABLE stedi.tmp_access_log (
            ip_address STRING,
            timestamp TIMESTAMP,
            method STRING,
            path STRING,
            protocol STRING,
            status_code BIGINT,
            response_size BIGINT
        )
        STORED AS PARQUET
        LOCATION '{access_staging_path}'
    """)

    run_query(f"""
        CREATE EXTERNAL TABLE stedi.tmp_error_log (
            timestamp TIMESTAMP,
            module STRING,
            pid STRING,
            message STRING
        )
        STORED AS PARQUET
        LOCATION '{error_staging_path}'
    """)



    
    # --------------------------------------------------
    # Create Iceberg access table
    # --------------------------------------------------

    run_query(f"""
        CREATE TABLE stedi.bronze_access_log (
            ip_address STRING,
            timestamp TIMESTAMP,
            method STRING,
            path STRING,
            protocol STRING,
            status_code BIGINT,
            response_size BIGINT
        )
        LOCATION '{access_iceberg_path}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'PARQUET'
        )
    """)

    # --------------------------------------------------
    # Create Iceberg error table
    # --------------------------------------------------

    run_query(f"""
        CREATE TABLE stedi.bronze_error_log (
            timestamp TIMESTAMP,
            module STRING,
            pid STRING,
            message STRING
        )
        LOCATION '{error_iceberg_path}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'PARQUET'
        )
    """)

    # --------------------------------------------------
    # Insert access data
    # --------------------------------------------------

    run_query("""
        INSERT INTO stedi.bronze_access_log

        SELECT
            ip_address,
            timestamp,
            method,
            path,
            protocol,
            status_code,
            response_size

        FROM stedi.tmp_access_log
    """)

    # --------------------------------------------------
    # Insert error data
    # --------------------------------------------------

    run_query("""
        INSERT INTO stedi.bronze_error_log

        SELECT
            timestamp,
            module,
            pid,
            message

        FROM stedi.tmp_error_log
    """)



    #---------------------------------------------------
    # Remove temporary Athena tables
    # --------------------------------------------------

    run_query("""
        DROP TABLE IF EXISTS stedi.tmp_access_log
    """)

    run_query("""
        DROP TABLE IF EXISTS stedi.tmp_error_log
    """)

    print(
        "Bronze Iceberg tables recreated "
        "and data inserted successfully."
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