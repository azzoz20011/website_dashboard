from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

import pendulum
import time


# --------------------------------------------------
# Configuration
# --------------------------------------------------

AWS_CONN_ID = "aws_default"
REGION = "eu-north-1"

BUCKET = "my-portfolio-bucket-6762"

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
    INSERT INTO {DATABASE}.access_log

    SELECT
        ip_address,
        timestamp,
        method,
        path,
        protocol,
        status_code,
        response_size

    FROM {DATABASE}.bronze_access_log
    """

    run_query(access_query)


    # ----------------------------------------------
    # Error logs
    # ----------------------------------------------

    error_query = f"""
    INSERT INTO {DATABASE}.error_log

    SELECT
        timestamp,
        module,
        pid,
        message

    FROM {DATABASE}.bronze_error_log
    """

    run_query(error_query)


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