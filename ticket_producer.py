{"id":"75921","variant":"standard","title":"Ticket Producer hybride local/cloud"}
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
import json
import time
import random
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# -----------------------------
# Configuration Kafka via .env
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_PROTOCOL = os.getenv("KAFKA_PROTOCOL", "PLAINTEXT")
KAFKA_MECHANISM = os.getenv("KAFKA_MECHANISM", None)
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", None)
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", None)

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'client.id': 'ticket-producer',
}

# Si on utilise SASL (cloud)
if KAFKA_PROTOCOL.upper().startswith("SASL"):
    conf['security.protocol'] = KAFKA_PROTOCOL
    conf['sasl.mechanisms'] = KAFKA_MECHANISM
    conf['sasl.username'] = KAFKA_USERNAME
    conf['sasl.password'] = KAFKA_PASSWORD

# -----------------------------
# Création du topic si nécessaire
# -----------------------------
admin_client = AdminClient(conf)
topic_name = "client_tickets"

try:
    topics = admin_client.list_topics(timeout=10).topics
except Exception as e:
    print(f"Impossible de récupérer les topics : {e}")
    topics = {}

if topic_name not in topics:
    print(f"Topic '{topic_name}' non trouvé, création...")
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' créé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la création du topic '{topic}': {e}")
else:
    print(f"Topic '{topic_name}' déjà existant.")

# -----------------------------
# Initialisation du producteur
# -----------------------------
producer = Producer(conf)
faker = Faker()
ticket_counter = 1

def generate_ticket():
    """Génère un ticket client aléatoire."""
    global ticket_counter
    ticket_id = f"TICKET-{ticket_counter:04d}"
    ticket_counter += 1

    date = faker.date_time_between(start_date=datetime(2025, 1, 1), end_date="now")
    
    ticket = {
        "ticket_id": ticket_id,
        "client_id": faker.random_int(min=1000, max=9999),
        "created_at": date.isoformat(),
        "request": faker.sentence(),
        "request_type": random.choice(["support", "billing", "technical", "other"]),
        "priority": random.choice(["low", "medium", "high"])
    }
    return ticket

def delivery_report(err, msg):
    """Callback après envoi d'un message."""
    if err:
        print(f"Erreur lors de l'envoi du message : {err}")
    else:
        print(f"Message envoyé : {msg.value().decode('utf-8')}")

# -----------------------------
# Envoi des tickets
# -----------------------------
nb_tickets = int(os.getenv("NB_TICKETS", 100))

for n in range(nb_tickets):
    ticket = generate_ticket()
    producer.produce(topic_name, key=str(ticket["ticket_id"]), value=json.dumps(ticket), callback=delivery_report)

producer.flush()
print(f"{nb_tickets} tickets ont été envoyés à Kafka !")
