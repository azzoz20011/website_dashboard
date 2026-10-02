from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


import pendulum
import pandas as pd
import geoip2.database
import ipaddress
import io


# ============================================================
# CONFIG
# ============================================================

STAGING_FILE = "/opt/airflow/data/access_log.parquet"

IP_LOCATION_FILE = "/opt/airflow/data/ip_location.parquet"
ACCESS_RAW_FILE = "/opt/airflow/data/access_log_raw.parquet"

GEOIP_DB = "/opt/airflow/data/GeoLite2-City.mmdb"


# ============================================================
# 1. READ STAGING + CREATE TWO DATAFRAMES
# ============================================================

def prepare_dataframes(**context):

    # Get Airflow DAG execution date
    logical_date = context["logical_date"].in_timezone("Asia/Riyadh")

    date_path = logical_date.format("YYYY/MM/DD")

    # Example:
    # staging/httpd/2026/10/01/access_log/access_log.parquet
    prefix = f"{STAGING_PREFIX}/{date_path}/access_log/"

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    # Find parquet file inside today's access_log folder
    keys = s3.list_keys(
        bucket_name=BUCKET,
        prefix=prefix
    )

    parquet_keys = [
        key for key in keys
        if key.endswith(".parquet")
    ]

    if not parquet_keys:
        raise FileNotFoundError(
            f"No parquet file found in "
            f"s3://{BUCKET}/{prefix}"
        )

    # If there is one access_log parquet
    staging_key = parquet_keys[0]

    print(
        f"Reading staging file: "
        f"s3://{BUCKET}/{staging_key}"
    )

    # Download parquet from S3
    obj = s3.get_key(
        key=staging_key,
        bucket_name=BUCKET
    )

    parquet_bytes = obj.get()["Body"].read()

    df = pd.read_parquet(
        io.BytesIO(parquet_bytes)
    )

    print(f"Total staging rows: {len(df)}")

    # ========================================================
    # DataFrame 1: Distinct IP addresses
    # ========================================================

    df_ips = (
        df[["ip_address"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # ========================================================
    # DataFrame 2: Access log
    # ========================================================

    df_access = df[
        [
            "ip_address",
            "timestamp",
            "method",
            "path",
            "protocol",
            "status_code",
            "response_size",
        ]
    ].copy()

    print(f"Distinct IPs: {len(df_ips)}")
    print(f"Access rows: {len(df_access)}")

    return df_ips, df_access

# ============================================================
# 2. GEOLOCATE DISTINCT IPs
# ============================================================

def enrich_ip_addresses():

    df_ips = pd.read_parquet(
        "/opt/airflow/data/distinct_ips.parquet"
    )

    reader = geoip2.database.Reader(GEOIP_DB)

    rows = []

    for ip in df_ips["ip_address"]:

        location = {
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

            # Ignore private/local IPs
            if not ip_obj.is_global:
                rows.append(location)
                continue

            response = reader.city(ip)

            location["country"] = response.country.name

            if response.subdivisions:
                location["region"] = (
                    response.subdivisions
                    .most_specific
                    .name
                )

            location["city"] = response.city.name

            location["latitude"] = (
                response.location.latitude
            )

            location["longitude"] = (
                response.location.longitude
            )

            location["timezone"] = (
                response.location.time_zone
            )

        except Exception as e:

            print(
                f"Could not geolocate {ip}: {e}"
            )

        rows.append(location)

    reader.close()

    df_ip_location = pd.DataFrame(rows)

    print(
        f"Generated {len(df_ip_location)} "
        f"IP location rows"
    )

    print(df_ip_location.head())

    df_ip_location.to_parquet(
        IP_LOCATION_FILE,
        index=False
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="process_access_log",
    start_date=pendulum.datetime(
        2026,
        10,
        1,
        tz="Asia/Riyadh"
    ),
    schedule=None,
    catchup=False,
    tags=["httpd", "geoip"],
) as dag:

    prepare = PythonOperator(
        task_id="prepare_dataframes",
        python_callable=prepare_dataframes,
    )

    geolocate = PythonOperator(
        task_id="geolocate_ips",
        python_callable=enrich_ip_addresses,
    )

    prepare >> geolocate