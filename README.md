# 🚀 InduTechData : Pipeline de Traitement de Tickets Kafka/Spark

## 📖 Présentation du projet
Ce projet implémente une architecture de traitement de données en temps réel (Streaming) pour automatiser le traitement de tickets clients. Il démontre la capacité à orchestrer des flux de données avec **Apache Kafka** et à les traiter massivement avec **PySpark**, le tout conteneurisé pour une portabilité totale.

## 🛠️ Architecture Technique
* **Ingestion (Temps réel) :** Producteur Python simulant des tickets clients envoyés vers **Redpanda** (API compatible Kafka).
* **Traitement (Batch/Streaming) :** **PySpark** pour la lecture du flux, le parsing JSON et l'enrichissement.
* **Stockage & Export :** Écriture double (Double Writing) vers deux destinations :
    1. **Détails :** Fichiers JSON partitionnés par `request_type`.
    2. **Résumé :** Statistiques de traitement groupées par type de ticket.
* **Interface :** **Redpanda Console** pour le monitoring des topics et des messages.

## 📂 Structure du Projet
* `ticket_producer.py` : Script de génération aléatoire de tickets clients avec `Faker`.
* `ticket_processor_export_container.py` : Script PySpark de traitement du flux et écriture S3/Local.
* `docker-compose.yml` : Orchestration des conteneurs (Redpanda, Producteur, Processeur).
* `Dockerfile.producer` / `Dockerfile.processor` : Images Docker optimisées (Python 3.9 slim).

## 🚀 Mise en route

### 1. Prérequis
* Docker et Docker-Compose installés.
* Python 3.9+

### 2. Configuration
Créez votre fichier `.env` à la racine en vous basant sur votre configuration (Kafka, AWS si utilisation Cloud) :
```bash
KAFKA_BOOTSTRAP=redpanda:9092
# Variables AWS optionnelles pour le mode Cloud
AWS_ACCESS_KEY_ID=votre_cle
AWS_SECRET_ACCESS_KEY=votre_secret
S3_BUCKET_NAME=nom_du_bucket