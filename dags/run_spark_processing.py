from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="run_spark_processing",
    start_date=datetime(2024, 3, 10),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark = SparkSubmitOperator(
        task_id="submit_spark_applications_processing",
        application="/opt/airflow/dags/spark_jobs/process_applications.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True
    )
