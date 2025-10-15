import os
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
import requests
import time
import psycopg2
from datetime import datetime

# Load environment variables from the root .env file
load_dotenv(dotenv_path='../.env')

def get_db_connection():
    """Establishes a connection to the Supabase PostgreSQL database."""
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    if not POSTGRES_URI:
        raise ValueError("POSTGRES_URI not found in .env file.")
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def get_user_features(conn, user_id):
    """Fetches a user's complete feature set from the database and maps columns to match the model's expectations."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE customerID = %s;", (user_id,))
    user_data = cursor.fetchone()
    
    if not user_data:
        cursor.close()
        return None

    db_columns = [desc[0].lower() for desc in cursor.description]
    cursor.close()
    
    user_df = pd.DataFrame([user_data], columns=db_columns)

    column_mapping = {
        'customerid': 'customerID', 'gender': 'gender', 'seniorcitizen': 'SeniorCitizen', 
        'partner': 'Partner', 'dependents': 'Dependents', 'tenure': 'tenure', 
        'phoneservice': 'PhoneService', 'multiplelines': 'MultipleLines', 
        'internetservice': 'InternetService', 'onlinesecurity': 'OnlineSecurity', 
        'onlinebackup': 'OnlineBackup', 'deviceprotection': 'DeviceProtection', 
        'techsupport': 'TechSupport', 'streamingtv': 'StreamingTV', 
        'streamingmovies': 'StreamingMovies', 'contract': 'Contract', 
        'paperlessbilling': 'PaperlessBilling', 'paymentmethod': 'PaymentMethod', 
        'monthlycharges': 'MonthlyCharges', 'totalcharges': 'TotalCharges'
    }
    user_df.rename(columns=column_mapping, inplace=True)
    
    return user_df

def log_event_to_db(conn, event):
    """Logs a raw event to the 'events' table."""
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO events (user_id, event_type, event_timestamp, event_data) VALUES (%s, %s, %s, %s)",
            (event.get('user_id'), event.get('event_type'), datetime.fromtimestamp(event.get('timestamp')), json.dumps(event))
        )
        conn.commit()

def log_prediction_to_db(conn, user_id, probability):
    """Logs a new prediction score to the 'predictions' table."""
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO predictions (user_id, churn_probability, prediction_timestamp) VALUES (%s, %s, %s)",
            (user_id, probability, datetime.now())
        )
        conn.commit()

def create_kafka_consumer(service_uri, topic_name):
    """Creates a Kafka consumer for Aiven with a group_id."""
    print("Connecting to Aiven Kafka for stream processing...")
    while True:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=service_uri,
                security_protocol="SSL",
                ssl_cafile="../ca.pem",
                ssl_certfile="../service.cert",
                ssl_keyfile="../service.key",
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=120000,
                group_id='churn_processor_group_v2' # Use a new group_id to reset offset
            )
            print("Stream processor connected to Aiven Kafka!")
            return consumer
        except Exception as e:
            print(f"Failed to connect: {e}. Retrying...")
            time.sleep(5)

def process_stream(consumer, model, db_conn):
    """Consumes events, fetches the updated user state, predicts, and logs."""
    print("Stream processor started. Listening for user events...")
    for message in consumer:
        event = message.value
        user_id = event.get('user_id')
        print(f"PROCESSOR: Received event '{event.get('event_type')}' for user {user_id}")

        try:
            log_event_to_db(db_conn, event)
            user_df = get_user_features(db_conn, user_id)
            if user_df is None:
                print(f"Warning: User {user_id} not found. Skipping.")
                continue

            prediction_df = user_df.drop('customerID', axis=1, errors='ignore')
            churn_probability = model.predict_proba(prediction_df)[:, 1][0]
            risk_score = float(churn_probability)

            log_prediction_to_db(db_conn, user_id, risk_score)
            
            # Create a combined payload for broadcasting all events
            # broadcast_payload = {**event, "churn_probability": risk_score}
            # requests.post("http://localhost:8000/api/broadcast-event", json=broadcast_payload)
            # Prepare the core data payload
            payload = {**event, "churn_probability": risk_score}

# Determine the event type based on the risk score
# Using the > 0.70 threshold for a high-risk "alert"
            if risk_score > 0.70:
                broadcast_data = {
                "type": "churn_alert",
                "payload": payload
                }
                print(f"PROCESSOR: Identified high-risk alert for user {user_id} (Score: {risk_score:.2f})")
            else:
                broadcast_data = {
                "type": "new_event",
                "payload": payload
                }


            requests.post("http://localhost:8000/api/broadcast-event", json=broadcast_data)
            
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            print(f"Database connection lost: {e}. Reconnecting...")
            if db_conn: db_conn.close()
            db_conn = get_db_connection()
        except Exception as e:
            print(f"An error occurred processing event for {user_id}: {e}")

if __name__ == "__main__":
    MODEL_PATH = '../ml_model/churn_model_xgb.pkl'
    KAFKA_TOPIC = "user_events_topic"
    AIVEN_SERVICE_URI = os.getenv("AIVEN_SERVICE_URI")

    if not AIVEN_SERVICE_URI:
        raise ValueError("AIVEN_SERVICE_URI not found in .env file.")

    churn_model = joblib.load(MODEL_PATH)
    print("Successfully loaded XGBoost model.")

    db_connection = get_db_connection()
    if not db_connection:
        raise Exception("Could not connect to the database. Exiting.")

    kafka_consumer = create_kafka_consumer(AIVEN_SERVICE_URI, KAFKA_TOPIC)
    
    try:
        process_stream(kafka_consumer, churn_model, db_connection)
    except KeyboardInterrupt:
        print("\nShutting down processor...")
    finally:
        if db_connection:
            db_connection.close()
            print("Database connection closed.")


