import random
import json
from faker import Faker
from confluent_kafka import Producer
import psycopg2

# Kafka config
producer = Producer({'bootstrap.servers': 'kafka:9092'})
APPLICATION_TOPIC = 'fake_applications'

# Faker pour générer de fausses données
faker = Faker("fr_FR")

def get_existing_job_ids():
    """Récupère les job_ids de la table job_posts depuis PostgreSQL."""
    connection = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='postgres',
        port='5432'
    )
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM job_posts;")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return [row[0] for row in rows]

def generate_random_application(job_id):
    """Génère une candidature aléatoire liée à un job_id."""
    full_name = faker.name()
    email = faker.email()
    resume_summary = faker.text(max_nb_chars=200)
    return {
        "job_id": job_id,
        "candidate_name": full_name,
        "email": email,
        "resume_summary": resume_summary
    }

def send_fake_applications(num=10):
    job_ids = get_existing_job_ids()
    if not job_ids:
        print("Aucun job_id trouvé. Assurez-vous que la table job_posts contient des données.")
        return

    for _ in range(num):
        job_id = random.choice(job_ids)
        app = generate_random_application(job_id)
        producer.produce(APPLICATION_TOPIC, json.dumps(app).encode("utf-8"))
        print(f"Candidature envoyée pour job_id={job_id} → {app['candidate_name']}")
    
    producer.flush()

if __name__ == "__main__":
    send_fake_applications(10)
