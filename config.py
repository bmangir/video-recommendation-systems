import os
from dotenv import load_dotenv

from db_connector.postgres_connector import PostgresConnector

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB_NAME = os.getenv("MONGODB_DB_NAME")

PG_USER = os.getenv("PG_DB_USER")
PG_PW = os.getenv("PG_DB_PW")
PG_PORT = os.getenv("PG_PORT")
PG_DB_NAME = os.getenv("PG_DB_NAME")
PG_DB_URL = os.getenv("PG_DB_URL")
# schemas
CORE_SCHEMA = os.getenv("PG_DB_CORE_SCHEMA")
ACTIVITY_SCHEMA = os.getenv("PG_DB_ACT_SCHEMA")
BATCH_OUTPUT_SCHEMA = os.getenv("PG_DB_OUTPUT_SCHEMA")

CONFLUENT_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET")
CONFLUENT_INTERACTION_TOPIC = os.getenv("CONFLUENT_INTERACTION_TOPIC")
CONFLUENT_CLIENT_ID = os.getenv("CONFLUENT_CLIENT_ID")

connector = PostgresConnector(PG_DB_NAME, PG_USER, PG_PW, PG_PORT)
conn = connector.create_connection()