from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

import pendulum
import time

import pandas as pd
import geoip2.database
import ipaddress

# --------------------------------------------------
# Configuration
# --------------------------------------------------

AWS_CONN_ID = "aws_default"
REGION = "us-east-1"

BUCKET = "my-portfolio-bucket-6762-us"

DATABASE = "stedi"

ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/"
)

ACCESS_BRONZE = (
    f"s3://{BUCKET}/bronze/httpd/access_log/"
)

ERROR_BRONZE = (
    f"s3://{BUCKET}/bronze/httpd/error_log/"
)

ACCESS_ICEBERG = (
    f"s3://{BUCKET}/silver/httpd/access_log/"
)

ERROR_ICEBERG = (
    f"s3://{BUCKET}/silver/httpd/error_log/"
)


# --------------------------------------------------
# Athena Client
# --------------------------------------------------

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

        time.sleep(2)


# ==================================================
# TASK 1
# Create database
# ==================================================

def create_database():

    query = f"""
    CREATE DATABASE IF NOT EXISTS {DATABASE}
    """

    run_query(query)


# ==================================================
# TASK 2
# Create Bronze external tables
# ==================================================

def create_bronze_tables():

    # ----------------------------------------------
    # Access log
    # ----------------------------------------------

    access_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS
    {DATABASE}.bronze_access_log (

        ip_address STRING,
        timestamp TIMESTAMP,
        method STRING,
        path STRING,
        protocol STRING,
        status_code INT,
        response_size BIGINT

    )

    STORED AS PARQUET

    LOCATION '{ACCESS_BRONZE}'
    """

    run_query(access_query)


    # ----------------------------------------------
    # Error log
    # ----------------------------------------------

    error_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS
    {DATABASE}.bronze_error_log (

        timestamp TIMESTAMP,
        module STRING,
        pid STRING,
        message STRING

    )

    STORED AS PARQUET

    LOCATION '{ERROR_BRONZE}'
    """

    run_query(error_query)


# ==================================================
# TASK 3
# Create Iceberg tables
# ==================================================

def create_iceberg_tables():

    # ----------------------------------------------
    # Access Iceberg table
    # ----------------------------------------------

    access_query = f"""
    CREATE TABLE IF NOT EXISTS
    {DATABASE}.access_log (

        ip_address STRING,
        timestamp TIMESTAMP,
        method STRING,
        path STRING,
        protocol STRING,
        status_code INT,
        response_size BIGINT

    )

    LOCATION '{ACCESS_ICEBERG}'

    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet'
    )
    """

    run_query(access_query)


    # ----------------------------------------------
    # Error Iceberg table
    # ----------------------------------------------

    error_query = f"""
    CREATE TABLE IF NOT EXISTS
    {DATABASE}.error_log (

        timestamp TIMESTAMP,
        module STRING,
        pid STRING,
        message STRING

    )

    LOCATION '{ERROR_ICEBERG}'

    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet'
    )
    """

    run_query(error_query)


# ==================================================
# TASK 4
# Load Bronze -> Iceberg
# ==================================================

def load_iceberg_tables():

    # ----------------------------------------------
    # Access logs
    # ----------------------------------------------

    access_query = f"""
    MERGE INTO {DATABASE}.access_log AS target
    USING {DATABASE}.bronze_access_log AS source

    ON  target.ip_address = source.ip_address
    AND target.timestamp = source.timestamp
    AND target.method = source.method
    AND target.path = source.path
    AND target.status_code = source.status_code

    WHEN MATCHED THEN
    UPDATE SET
        protocol = source.protocol,
        response_size = source.response_size

    WHEN NOT MATCHED THEN
    INSERT (
        ip_address,
        timestamp,
        method,
        path,
        protocol,
        status_code,
        response_size
    )
    VALUES (
        source.ip_address,
        source.timestamp,
        source.method,
        source.path,
        source.protocol,
        source.status_code,
        source.response_size
    )
    """

    run_query(access_query)



    # ----------------------------------------------
    # Error logs
    # ----------------------------------------------

    error_query = f"""
    MERGE INTO {DATABASE}.error_log AS target
    USING {DATABASE}.bronze_error_log AS source

    ON  target.timestamp = source.timestamp
    AND target.module = source.module
    AND target.pid = source.pid
    AND target.message = source.message

    WHEN MATCHED THEN
    UPDATE SET
        message = source.message

    WHEN NOT MATCHED THEN
    INSERT (
        timestamp,
        module,
        pid,
        message
    )
    VALUES (
        source.timestamp,
        source.module,
        source.pid,
        source.message
    )
    """

    run_query(error_query)

def enrich_ip_addresses(
    input_parquet,
    geoip_db="/path/to/GeoLite2-City.mmdb"
):
    df = pd.read_parquet(input_parquet)

    # Keep only unique IP addresses
    ips = (
        df[["ip_address"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    reader = geoip2.database.Reader(geoip_db)

    def lookup(ip):
        try:
            # Validate IP first
            ipaddress.ip_address(ip)

            response = reader.city(ip)

            return pd.Series({
                "country": response.country.name,
                "region": (
                    response.subdivisions.most_specific.name
                    if response.subdivisions
                    else None
                ),
                "city": response.city.name,
                "latitude": response.location.latitude,
                "longitude": response.location.longitude,
                "timezone": response.location.time_zone,
            })

        except Exception:
            return pd.Series({
                "country": None,
                "region": None,
                "city": None,
                "latitude": None,
                "longitude": None,
                "timezone": None,
            })

    geo_data = ips["ip_address"].apply(lookup)

    result = pd.concat(
        [ips, geo_data],
        axis=1
    )

    reader.close()

    return result
# ==================================================
# DAG
# ==================================================

with DAG(

    dag_id="httpd_bronze_to_silver",

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
        "iceberg",
        "silver",
        "athena"
    ],

) as dag:


    create_db = PythonOperator(
        task_id="create_stedi_database",
        python_callable=create_database
    )


    create_bronze = PythonOperator(
        task_id="create_bronze_tables",
        python_callable=create_bronze_tables
    )


    create_iceberg = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables
    )


    load_data = PythonOperator(
        task_id="load_bronze_to_iceberg",
        python_callable=load_iceberg_tables
    )


    create_db >> create_bronze >> create_iceberg >> load_data