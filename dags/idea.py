from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator   

import pendulum
import pandas as pd
import geoip2.database
import io
import ipaddress

import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
)

# ============================================================
# CONFIG
# ============================================================

BUCKET = "my-portfolio-bucket-6762-us"
AWS_CONN_ID = "aws_default"

STAGING_PREFIX = "staging/httpd"
SILVER_PREFIX = "silver/httpd"

DATABASE = "stedi"

ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/"
)

ICEBERG_PREFIX = (
    f"s3://{BUCKET}/iceberg/httpd"
)

# GeoLite2 database inside Airflow container
GEOIP_DB = "/opt/airflow/data/GeoLite2-City.mmdb"


# ============================================================
# HELPER: RUN ATHENA QUERY
# ============================================================

def run_athena_query(query):

    athena = AthenaHook(
        aws_conn_id=AWS_CONN_ID
    )

    query_id = athena.run_query(
        query=query,
        query_context={
            "Database": DATABASE
        },
        result_configuration={
            "OutputLocation": ATHENA_OUTPUT
        },
    )

    athena.poll_query_status(query_id)

    print(f"Athena query finished: {query_id}")


# ============================================================
# TASK 1
# Read today's access_log parquet
#
# Output:
#   distinct_ips.parquet
#   all_ips.parquet
# ============================================================

def prepare_ip_files(**context):

    logical_date = context["logical_date"].in_timezone(
        "Asia/Riyadh"
    )

    date_path = logical_date.format("YYYY/MM/DD")

    input_prefix = (
        f"{STAGING_PREFIX}/"
        f"{date_path}/access_log/"
    )

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    # --------------------------------------------------------
    # Find parquet file
    # --------------------------------------------------------

    keys = s3.list_keys(
        bucket_name=BUCKET,
        prefix=input_prefix
    ) or []

    parquet_keys = [
        key
        for key in keys
        if key.endswith(".parquet")
    ]

    if not parquet_keys:
        raise FileNotFoundError(
            f"No parquet file found in "
            f"s3://{BUCKET}/{input_prefix}"
        )

    # --------------------------------------------------------
    # Read all parquet files in today's folder
    # --------------------------------------------------------

    dataframes = []

    for key in parquet_keys:

        print(
            f"Reading s3://{BUCKET}/{key}"
        )

        obj = s3.get_key(
            key=key,
            bucket_name=BUCKET
        )

        data = obj.get()["Body"].read()

        df_part = pd.read_parquet(
            io.BytesIO(data)
        )

        dataframes.append(df_part)

    df = pd.concat(
        dataframes,
        ignore_index=True
    )

    print(f"Total rows: {len(df)}")

    # --------------------------------------------------------
    # distinct_ips
    # --------------------------------------------------------

    distinct_ips = (
        df[["ip_address"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # all_ips
    # --------------------------------------------------------

    all_ips = (
        df[
            [
                "ip_address",
                "timestamp"
            ]
        ]
        .dropna(subset=["ip_address"])
        .reset_index(drop=True)
    )

    print(
        f"Distinct IPs: {len(distinct_ips)}"
    )

    print(
        f"All IP rows: {len(all_ips)}"
    )

    # --------------------------------------------------------
    # Convert DataFrames to parquet
    # --------------------------------------------------------

    distinct_buffer = io.BytesIO()

    distinct_ips.to_parquet(
        distinct_buffer,
        index=False
    )

    all_buffer = io.BytesIO()

    all_ips.to_parquet(
        all_buffer,
        index=False
    )

    # --------------------------------------------------------
    # Upload to S3
    # --------------------------------------------------------

    distinct_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"distinct_ips.parquet"
    )

    all_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"all_ips.parquet"
    )

    s3.load_bytes(
        bytes_data=distinct_buffer.getvalue(),
        key=distinct_key,
        bucket_name=BUCKET,
        replace=True,
    )

    s3.load_bytes(
        bytes_data=all_buffer.getvalue(),
        key=all_key,
        bucket_name=BUCKET,
        replace=True,
    )

    print(
        f"Created: s3://{BUCKET}/{distinct_key}"
    )

    print(
        f"Created: s3://{BUCKET}/{all_key}"
    )


# ============================================================
# TASK 2
# Read distinct_ips
# Get location using GeoIP2
#
# Output:
#   location_ips.parquet
# ============================================================

def create_location_file(**context):

    logical_date = context["logical_date"].in_timezone(
        "Asia/Riyadh"
    )

    date_path = logical_date.format("YYYY/MM/DD")

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    distinct_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"distinct_ips.parquet"
    )

    # --------------------------------------------------------
    # Read distinct IPs
    # --------------------------------------------------------

    obj = s3.get_key(
        key=distinct_key,
        bucket_name=BUCKET
    )

    data = obj.get()["Body"].read()

    distinct_ips = pd.read_parquet(
        io.BytesIO(data)
    )

    # --------------------------------------------------------
    # GeoIP lookup
    # --------------------------------------------------------

    reader = geoip2.database.Reader(
        GEOIP_DB
    )

    rows = []

    for ip in distinct_ips["ip_address"]:

        row = {
            "ip_address": ip,
            "country": None,
            "region": None,
            "city": None,
            "latitude": None,
            "longitude": None,
            "timezone": None,
        }

        try:

            ip_obj = ipaddress.ip_address(ip)

            # Private/local IP
            if not ip_obj.is_global:
                rows.append(row)
                continue

            response = reader.city(ip)

            row["country"] = (
                response.country.name
            )

            row["region"] = (
                response.subdivisions
                .most_specific
                .name
            )

            row["city"] = (
                response.city.name
            )

            row["latitude"] = (
                response.location.latitude
            )

            row["longitude"] = (
                response.location.longitude
            )

            row["timezone"] = (
                response.location.time_zone
            )

        except Exception as e:

            print(
                f"Could not locate {ip}: {e}"
            )

        rows.append(row)

    reader.close()

    location_ips = pd.DataFrame(rows)

    print(location_ips.head())

    # --------------------------------------------------------
    # Write parquet
    # --------------------------------------------------------

    buffer = io.BytesIO()

    location_ips.to_parquet(
        buffer,
        index=False
    )

    location_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"location_ips.parquet"
    )

    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=location_key,
        bucket_name=BUCKET,
        replace=True,
    )

    print(
        f"Created: "
        f"s3://{BUCKET}/{location_key}"
    )


# ============================================================
# TASK 3
#
# Create:
#
# stedi.location_ips
# stedi.all_ips
#
# Then join them into:
#
# stedi.location_ip
# ============================================================
def create_iceberg_tables(**context):

    logical_date = context["logical_date"].in_timezone(
        "Asia/Riyadh"
    )

    date_path = logical_date.format("YYYY/MM/DD")

    location_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"location_ips.parquet"
    )

    all_ips_key = (
        f"{SILVER_PREFIX}/"
        f"{date_path}/"
        f"all_ips.parquet"
    )

    # ========================================================
    # 1. Read today's Parquet files from S3
    # ========================================================

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    location_obj = s3.get_key(
        key=location_key,
        bucket_name=BUCKET,
    )

    location_df = pd.read_parquet(
        io.BytesIO(
            location_obj.get()["Body"].read()
        )
    )

    all_ips_obj = s3.get_key(
        key=all_ips_key,
        bucket_name=BUCKET,
    )

    all_ips_df = pd.read_parquet(
        io.BytesIO(
            all_ips_obj.get()["Body"].read()
        )
    )

    print(
        f"location_ips rows: {len(location_df)}"
    )

    print(
        f"all_ips rows: {len(all_ips_df)}"
    )

    # ========================================================
    # 2. Get AWS credentials from Airflow
    # ========================================================

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    session = s3.get_session()

    credentials = session.get_credentials().get_frozen_credentials()
    region = session.region_name or "us-east-1"

    catalog_properties = {
        "type": "glue",

        # Glue Catalog credentials
        "client.access-key-id": credentials.access_key,
        "client.secret-access-key": credentials.secret_key,
        "client.region": region,

        # S3 credentials
        "s3.access-key-id": credentials.access_key,
        "s3.secret-access-key": credentials.secret_key,
        "s3.region": region,
    }

    if credentials.token:
        catalog_properties["client.session-token"] = credentials.token
        catalog_properties["s3.session-token"] = credentials.token

    catalog = load_catalog(
        "glue",
        **catalog_properties,
    )

    # ========================================================
    # 3. Connect PyIceberg to AWS Glue Catalog
    # ========================================================

    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",

            # Glue credentials
            "client.access-key-id": credentials.access_key,
            "client.secret-access-key": credentials.secret_key,
            "client.region": session.region_name,

            # S3 credentials
            "s3.access-key-id": credentials.access_key,
            "s3.secret-access-key": credentials.secret_key,
            "s3.region": session.region_name,
        },
    )
    # ========================================================
    # 4. Create namespace/database
    # ========================================================

    try:
        catalog.create_namespace(DATABASE)
        print(
            f"Created namespace: {DATABASE}"
        )

    except Exception:
        print(
            f"Namespace {DATABASE} already exists"
        )

    # ========================================================
    # 5. Schemas
    # ========================================================

    location_schema = Schema(

        NestedField(
            1,
            "ip_address",
            StringType(),
            required=False,
        ),

        NestedField(
            2,
            "country",
            StringType(),
            required=False,
        ),

        NestedField(
            3,
            "region",
            StringType(),
            required=False,
        ),

        NestedField(
            4,
            "city",
            StringType(),
            required=False,
        ),

        NestedField(
            5,
            "latitude",
            DoubleType(),
            required=False,
        ),

        NestedField(
            6,
            "longitude",
            DoubleType(),
            required=False,
        ),

        NestedField(
            7,
            "timezone",
            StringType(),
            required=False,
        ),
    )

    all_ips_schema = Schema(

        NestedField(
            1,
            "ip_address",
            StringType(),
            required=False,
        ),

        NestedField(
            2,
            "timestamp",
            StringType(),
            required=False,
        ),
    )

    final_schema = Schema(

        NestedField(
            1,
            "ip_address",
            StringType(),
            required=False,
        ),

        NestedField(
            2,
            "timestamp",
            StringType(),
            required=False,
        ),

        NestedField(
            3,
            "country",
            StringType(),
            required=False,
        ),

        NestedField(
            4,
            "region",
            StringType(),
            required=False,
        ),

        NestedField(
            5,
            "city",
            StringType(),
            required=False,
        ),

        NestedField(
            6,
            "latitude",
            DoubleType(),
            required=False,
        ),

        NestedField(
            7,
            "longitude",
            DoubleType(),
            required=False,
        ),

        NestedField(
            8,
            "timezone",
            StringType(),
            required=False,
        ),
    )

    # ========================================================
    # 6. Create/load location_ips
    # ========================================================

    try:

        location_table = catalog.load_table(
            f"{DATABASE}.location_ips"
        )

        print(
            "location_ips already exists"
        )

    except Exception:

        location_table = catalog.create_table(
            identifier=f"{DATABASE}.location_ips",
            schema=location_schema,
            location=(
                f"{ICEBERG_PREFIX}/"
                f"location_ips/"
            ),
        )

        print(
            "Created location_ips"
        )

    # ========================================================
    # 7. Create/load all_ips
    # ========================================================

    try:

        all_ips_table = catalog.load_table(
            f"{DATABASE}.all_ips"
        )

        print(
            "all_ips already exists"
        )

    except Exception:

        all_ips_table = catalog.create_table(
            identifier=f"{DATABASE}.all_ips",
            schema=all_ips_schema,
            location=(
                f"{ICEBERG_PREFIX}/"
                f"all_ips/"
            ),
        )

        print(
            "Created all_ips"
        )

    # ========================================================
    # 8. Prepare DataFrames
    # ========================================================

    location_df = location_df[
        [
            "ip_address",
            "country",
            "region",
            "city",
            "latitude",
            "longitude",
            "timezone",
        ]
    ]

    all_ips_df = all_ips_df[
        [
            "ip_address",
            "timestamp",
        ]
    ]

    # Force expected data types

    for column in [
        "ip_address",
        "country",
        "region",
        "city",
        "timezone",
    ]:
        location_df[column] = (
            location_df[column]
            .astype("string")
        )

    location_df["latitude"] = pd.to_numeric(
        location_df["latitude"],
        errors="coerce",
    )

    location_df["longitude"] = pd.to_numeric(
        location_df["longitude"],
        errors="coerce",
    )

    all_ips_df["ip_address"] = (
        all_ips_df["ip_address"]
        .astype("string")
    )

    all_ips_df["timestamp"] = (
        all_ips_df["timestamp"]
        .astype("string")
    )

    # ========================================================
    # 9. Convert Pandas -> Arrow
    # ========================================================

    location_arrow = pa.Table.from_pandas(
        location_df,
        preserve_index=False,
    )

    all_ips_arrow = pa.Table.from_pandas(
        all_ips_df,
        preserve_index=False,
    )

    # ========================================================
    # 10. Append into Iceberg
    # ========================================================

    location_table.append(
        location_arrow
    )

    print(
        f"Inserted {len(location_df)} rows "
        "into location_ips"
    )

    all_ips_table.append(
        all_ips_arrow
    )

    print(
        f"Inserted {len(all_ips_df)} rows "
        "into all_ips"
    )

    # ========================================================
    # 11. Create final location_ip Iceberg table
    # ========================================================

    try:

        catalog.load_table(
            f"{DATABASE}.location_ip"
        )

        print(
            "location_ip already exists"
        )

    except Exception:

        catalog.create_table(
            identifier=f"{DATABASE}.location_ip",
            schema=final_schema,
            location=(
                f"{ICEBERG_PREFIX}/"
                f"location_ip/"
            ),
        )

        print(
            "Created location_ip"
        )

    # ========================================================
    # 12. Join using Athena
    # ========================================================

    run_athena_query(
        f"""
        MERGE INTO {DATABASE}.location_ip AS target

        USING (
            SELECT
                a.ip_address,
                a.timestamp,
                l.country,
                l.region,
                l.city,
                l.latitude,
                l.longitude,
                l.timezone

            FROM {DATABASE}.all_ips AS a

            LEFT JOIN {DATABASE}.location_ips AS l
                ON a.ip_address = l.ip_address

        ) AS source

        ON  target.ip_address = source.ip_address
        AND target.timestamp = source.timestamp

        WHEN MATCHED THEN
            UPDATE SET
                country   = source.country,
                region    = source.region,
                city      = source.city,
                latitude  = source.latitude,
                longitude = source.longitude,
                timezone  = source.timezone

        WHEN NOT MATCHED THEN
            INSERT (
                ip_address,
                timestamp,
                country,
                region,
                city,
                latitude,
                longitude,
                timezone
            )
            VALUES (
                source.ip_address,
                source.timestamp,
                source.country,
                source.region,
                source.city,
                source.latitude,
                source.longitude,
                source.timezone
            )
        """
    )



    print(
        "location_ip populated successfully"
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="ip_location_pipeline",

    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="Asia/Riyadh"
    ),

    schedule=None,

    catchup=False,

    tags=[
        "httpd",
        "ip",
        "geoip",
        "iceberg"
    ],

) as dag:

    prepare_files = PythonOperator(
        task_id="prepare_ip_files",
        python_callable=prepare_ip_files,
    )

    create_locations = PythonOperator(
        task_id="create_location_file",
        python_callable=create_location_file,
    )

    create_tables = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables,
    )

    prepare_files >> create_locations >> create_tables