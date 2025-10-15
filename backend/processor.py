import os
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
import requests
import time
import psycopg2
from datetime import datetime
import pytz

# --- SSL Certificate Handling ---
def create_ssl_files_from_env():
    """
    Reads SSL certificate content from environment variables and writes them to
    temporary files for the Kafka consumer to use.
    """
    print("PROCESSOR: Creating SSL files from environment variables...")
    ssl_files = {
        "ca.pem": "AIVEN_KAFKA_CA",
        "service.cert": "AIVEN_KAFKA_CERT",
        "service.key": "AIVEN_KAFKA_KEY"
    }
    for file_path, env_var_name in ssl_files.items():
        content = os.getenv(env_var_name)
        if not content:
            raise ValueError(f"Environment variable {env_var_name} is not set for Kafka.")
        with open(file_path, "w") as f:
            f.write(content.replace('\\n', '\n'))
    print("PROCESSOR: SSL files created successfully.")

# --- Database Functions ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    if not POSTGRES_URI:
        raise ValueError("POSTGRES_URI not found in .env file.")
    try:
        return psycopg2.connect(POSTGRES_URI)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def log_event_to_db(conn, event):
    """Logs a raw event to the 'events' table."""
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO events (user_id, event_type, event_timestamp, event_data) VALUES (%s, %s, %s, %s)",
            (event.get('user_id'), event.get('event_type'), datetime.fromtimestamp(event.get('timestamp')), json.dumps(event))
        )
        conn.commit()

def log_prediction_to_db(conn, user_id, probability):
    """Logs a new prediction score to the 'predictions' table using the correct IST timezone."""
    with conn.cursor() as cursor:
        ist = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist)
        cursor.execute(
            "INSERT INTO predictions (user_id, churn_probability, prediction_timestamp) VALUES (%s, %s, %s)",
            (user_id, probability, now_ist)
        )
        conn.commit()

# --- Feature Fetching ---
def get_user_features(conn, user_id):
    # ... (This function remains unchanged from your version)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE customerID = %s;", (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        cursor.close()
        return None
    db_columns = [desc[0].lower() for desc in cursor.description]
    cursor.close()
    user_df = pd.DataFrame([user_data], columns=db_columns)
    column_mapping = {'customerid': 'customerID', 'gender': 'gender', 'seniorcitizen': 'SeniorCitizen', 'partner': 'Partner', 'dependents': 'Dependents', 'tenure': 'tenure', 'phoneservice': 'PhoneService', 'multiplelines': 'MultipleLines', 'internetservice': 'InternetService', 'onlinesecurity': 'OnlineSecurity', 'onlinebackup': 'OnlineBackup', 'deviceprotection': 'DeviceProtection', 'techsupport': 'TechSupport', 'streamingtv': 'StreamingTV', 'streamingmovies': 'StreamingMovies', 'contract': 'Contract', 'paperlessbilling': 'PaperlessBilling', 'paymentmethod': 'PaymentMethod', 'monthlycharges': 'MonthlyCharges', 'totalcharges': 'TotalCharges'}
    user_df.rename(columns=column_mapping, inplace=True)
    return user_df

# --- Kafka Functions ---
def create_kafka_consumer(service_uri, topic_name):
    """Creates a Kafka consumer for Aiven, creating SSL files first."""
    create_ssl_files_from_env()
    print("PROCESSOR: Connecting to Aiven Kafka...")
    # ... (This function remains unchanged from your version, but ensure create_ssl_files_from_env is called)
    while True:
        try:
            consumer = KafkaConsumer(topic_name, bootstrap_servers=service_uri, security_protocol="SSL", ssl_cafile="ca.pem", ssl_certfile="service.cert", ssl_keyfile="service.key", auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')), request_timeout_ms=120000, group_id='churn_processor_group_v2')
            print("PROCESSOR: Stream processor connected to Aiven Kafka!")
            return consumer
        except Exception as e:
            print(f"PROCESSOR: Failed to connect: {e}. Retrying...")
            time.sleep(5)

# --- Main Stream Processing Logic ---
def process_stream(consumer, model, db_conn):
    """Consumes events, fetches updated user state, predicts, logs, and broadcasts."""
    print("PROCESSOR: Stream processor started. Listening for user events...")
    for message in consumer:
        event = message.value
        user_id = event.get('user_id')
        print(f"PROCESSOR: Received event '{event.get('event_type')}' for user {user_id}")
        try:
            log_event_to_db(db_conn, event)
            user_df = get_user_features(db_conn, user_id)
            if user_df is None:
                print(f"PROCESSOR: Warning: User {user_id} not found. Skipping.")
                continue
            
            prediction_df = user_df.drop('customerID', axis=1, errors='ignore')
            churn_probability = model.predict_proba(prediction_df)[:, 1][0]
            risk_score = float(churn_probability)
            
            log_prediction_to_db(db_conn, user_id, risk_score)
            
            payload = {**event, "churn_probability": risk_score}
            
            if risk_score > 0.70:
                broadcast_data = {"type": "churn_alert", "payload": payload}
                print(f"PROCESSOR: Identified high-risk alert for user {user_id} (Score: {risk_score:.2f})")
            else:
                broadcast_data = {"type": "new_event", "payload": payload}
            
            requests.post("http://localhost:8000/api/broadcast-event", json=broadcast_data)

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            print(f"PROCESSOR: Database connection lost: {e}. Reconnecting...")
            if db_conn: db_conn.close()
            db_conn = get_db_connection()
        except Exception as e:
            print(f"PROCESSOR: An error occurred processing event for {user_id}: {e}")