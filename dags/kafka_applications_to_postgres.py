from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Consumer
import psycopg2
import json

def consume_applications():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'airflow_applications_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['fake_applications'])

    connection = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='postgres',
        port='5432'
    )
    cursor = connection.cursor()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break

        data = json.loads(msg.value().decode('utf-8'))
        print("Nouvelle candidature :", data)

        cursor.execute("""
            INSERT INTO applications (job_id, candidate_name, email, resume_summary)
            VALUES (%s, %s, %s, %s)
        """, (data['job_id'], data['candidate_name'], data['email'], data['resume_summary']))

        connection.commit()

    cursor.close()
    connection.close()
    consumer.close()

with DAG(
    dag_id="consume_fake_applications",
    start_date=datetime(2025, 3, 23),
    schedule_interval=None,
    catchup=False,
    description="Consomme les candidatures fictives Kafka et les insère en base"
) as dag:

    task_consume_applications = PythonOperator(
        task_id="consume_kafka_applications",
        python_callable=consume_applications
    )
