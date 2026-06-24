import os
import sys
from dotenv import load_dotenv

# Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

load_dotenv()

# ---------------------------------------------------------
# 1. CONFIGURATION DE L'ENVIRONNEMENT
# ---------------------------------------------------------
if os.name == 'nt':
    print("Mode : Windows Local détecté")
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
else:
    print("Mode : Linux/Docker détecté")

packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'

# ---------------------------------------------------------
# 2. INITIALISATION SPARK
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("TicketProcessorAdvanced") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3. CONFIGURATION DESTINATIONS
# ---------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
bucket_name = os.getenv("S3_BUCKET_NAME")

# Définition des chemins de sortie
if bucket_name and os.getenv("AWS_ACCESS_KEY_ID"):
    BASE_PATH = f"s3a://{bucket_name}/output_analysis"
    CHECKPOINT_PATH = f"s3a://{bucket_name}/checkpoints"
    spark.conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    spark.conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    spark.conf.set("fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION', 'eu-west-3')}.amazonaws.com")
    print(f"Mode CLOUD activé -> S3: {BASE_PATH}")
else:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    BASE_PATH = "/app/output_analysis" if os.name != 'nt' else os.path.join(current_dir, "output_analysis")
    CHECKPOINT_PATH = os.path.join(current_dir, "checkpoints")
    print(f"Mode LOCAL activé -> {BASE_PATH}")

# ---------------------------------------------------------
# 4. LECTURE DU FLUX KAFKA
# ---------------------------------------------------------
print("📡 Connexion au flux Kafka...")
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

# Définition du schéma JSON
schema = (StructType()
    .add("ticket_id", StringType())
    .add("client_id", IntegerType())
    .add("created_at", StringType())
    .add("request", StringType())
    .add("request_type", StringType())
    .add("priority", StringType())
)

# Parsing et nettoyage
df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Ajout d'une colonne calculée 'processed_at'
df_enriched = df_parsed.withColumn("processed_at", current_timestamp())

# ---------------------------------------------------------
# 5. FONCTION DE TRAITEMENT (DOUBLE ÉCRITURE)
# ---------------------------------------------------------
def process_batch(df, epoch_id):
    """
    Cette fonction est appelée à chaque micro-batch.
    Elle effectue deux écritures distinctes.
    """
    # Optimisation : On met le DataFrame en cache car on va l'utiliser 2 fois
    df.persist()
    
    count = df.count()
    if count > 0:
        print(f"[Batch {epoch_id}] Traitement de {count} tickets...")

        # --- A. ÉCRITURE DES DÉTAILS (LISTE COMPLÈTE) ---
        # On organise les fichiers par dossiers selon le type de requête
        path_details = f"{BASE_PATH}/tickets_details"
        
        df.write \
            .format("json") \
            .mode("append") \
            .partitionBy("request_type") \
            .save(path_details)
        
        print(f"   Détails sauvegardés dans : {path_details}")

        # --- B. ÉCRITURE DU RÉCAPITULATIF (COUNT) ---
        # On calcule les stats sur ce batch précis
        df_summary = df.groupBy("request_type").count()
        
        # On sauvegarde le récap dans un dossier spécifique par Batch ID pour ne pas écraser l'historique
        path_summary = f"{BASE_PATH}/summaries/batch_{epoch_id}"
        
        df_summary.write \
            .format("json") \
            .mode("overwrite") \
            .save(path_summary)
            
        print(f"Récap sauvegardé dans : {path_summary}")
        df_summary.show() # Affiche le tableau dans la console pour la démo

    else:
        print(f"[Batch {epoch_id}] Aucun nouveau ticket.")
    
    # On libère la mémoire
    df.unpersist()

# ---------------------------------------------------------
# 6. LANCEMENT DU STREAMING
# ---------------------------------------------------------
print("▶Lancement du processeur...")

query = df_enriched.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()