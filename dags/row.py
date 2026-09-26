from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator   
import pendulum


BUCKET = "my-portfolio-bucket-6762-us"
AWS_CONN_ID = "aws_default"
SSH_CONN_ID = "ec2_ssh"


def verify_upload():
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    today = pendulum.now("Asia/Riyadh").format("YYYY/MM/DD")
    prefix = f"raw/httpd/{today}/"

    expected_files = [
        f"{prefix}access_log",
        f"{prefix}error_log",
    ]

    for key in expected_files:
        if not s3.check_for_key(
            key=key,
            bucket_name=BUCKET,
        ):
            raise ValueError(
                f"Missing file: s3://{BUCKET}/{key}"
            )

        print(f"Verified: s3://{BUCKET}/{key}")


with DAG(
    dag_id="upload_httpd_logs_to_s3",
    start_date=pendulum.datetime(
        2026, 9, 1,
        tz="Asia/Riyadh"
    ),
    schedule="0 1 * * *",
    catchup=False,
) as dag:

    check_ssh = SSHOperator(
        task_id="check_ssh_connection",
        ssh_conn_id=SSH_CONN_ID,
        command="""
        echo "SSH connection successful"
        whoami
        hostname
        """,
    )

    upload_logs = SSHOperator(
        task_id="upload_logs_to_s3",
        ssh_conn_id=SSH_CONN_ID,
        command="""
        DATE=$(date +%Y/%m/%d)

        aws s3 cp /var/log/httpd/access_log \
        s3://my-portfolio-bucket-6762/raw/httpd/$DATE/access_log

        aws s3 cp /var/log/httpd/error_log \
        s3://my-portfolio-bucket-6762/raw/httpd/$DATE/error_log
        """,
    )

    verify_s3 = PythonOperator(
        task_id="verify_s3_upload",
        python_callable=verify_upload,
    )


    trigger_cleaning = TriggerDagRunOperator(
        task_id="trigger_cleaning_dag",
        trigger_dag_id="httpd_raw_to_bronze",
    )

    check_ssh >> upload_logs >> verify_s3 >> trigger_cleaning   