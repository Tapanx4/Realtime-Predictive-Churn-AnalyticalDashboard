import os
import json
import random
import time
from kafka import KafkaProducer
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables from the root .env file
load_dotenv(dotenv_path='../.env')

def get_db_connection():
    """Establishes a connection to the Supabase PostgreSQL database."""
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    if not POSTGRES_URI: raise ValueError("POSTGRES_URI not found.")
    return psycopg2.connect(POSTGRES_URI)

def get_user_ids_from_db(conn):
    """Fetches a list of valid customer IDs from the database."""
    print("Fetching user IDs from PostgreSQL...")
    with conn.cursor() as cursor:
        cursor.execute("SELECT customerID FROM users;")
        user_ids = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(user_ids)} users.")
    return user_ids

def create_kafka_producer(service_uri):
    """Creates a Kafka producer for Aiven."""
    print("Connecting to Aiven Kafka...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=service_uri,
                security_protocol="SSL", ssl_cafile="../ca.pem",
                ssl_certfile="../service.cert", ssl_keyfile="../service.key",
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=120000
            )
            print("Event simulator connected to Aiven Kafka!")
            return producer
        except Exception as e:
            print(f"Failed to connect to Aiven Kafka: {e}. Retrying...")
            time.sleep(5)

def simulate_event(conn, user_id):
    """Selects a random event, applies the change to the database, and returns the event details."""
    event_handlers = [
        # Churn-increasing events (higher probability)
        _handle_contract_downgrade,
        _handle_service_removal,
        _handle_cancel_autopay,
        # Churn-decreasing events (lower probability)
        _handle_contract_upgrade,
        _handle_add_service,
        _handle_enable_autopay,
        # Neutral event
        _handle_tenure_increase,
    ]
    # Adjust weights to make negative events more common for demonstration
    weights = [0.25, 0.20, 0.15, 0.10, 0.10, 0.10, 0.10]
    
    handler = random.choices(event_handlers, weights=weights, k=1)[0]
    return handler(conn, user_id)

# --- CHURN-INCREASING EVENT HANDLERS ---

def _handle_contract_downgrade(conn, user_id):
    """Simulates a user downgrading from a yearly to a month-to-month contract."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET Contract = 'Month-to-month' WHERE customerID = %s AND Contract != 'Month-to-month';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'contract_downgrade', 'user_id': user_id, 'details': 'Switched to Month-to-month'}
    return None

def _handle_service_removal(conn, user_id):
    """Simulates a user removing a service like OnlineSecurity."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET OnlineSecurity = 'No', MonthlyCharges = MonthlyCharges - 5 WHERE customerID = %s AND OnlineSecurity = 'Yes';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'removed_online_security', 'user_id': user_id, 'details': 'Cancelled Online Security'}
    return None

def _handle_cancel_autopay(conn, user_id):
    """Simulates a user cancelling automatic payments."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET PaymentMethod = 'Mailed check' WHERE customerID = %s AND PaymentMethod LIKE '%%(automatic)%%';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'cancelled_autopay', 'user_id': user_id, 'details': 'Switched to Mailed check'}
    return None

# --- CHURN-DECREASING EVENT HANDLERS (NEW) ---

def _handle_contract_upgrade(conn, user_id):
    """Simulates a user upgrading from a monthly to a one-year contract."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET Contract = 'One year' WHERE customerID = %s AND Contract = 'Month-to-month';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'contract_upgrade', 'user_id': user_id, 'details': 'Upgraded to One year contract'}
    return None

def _handle_add_service(conn, user_id):
    """Simulates a user adding a service like TechSupport."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET TechSupport = 'Yes', MonthlyCharges = MonthlyCharges + 5 WHERE customerID = %s AND TechSupport = 'No';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'added_tech_support', 'user_id': user_id, 'details': 'Subscribed to Tech Support'}
    return None

def _handle_enable_autopay(conn, user_id):
    """Simulates a user enabling automatic payments."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET PaymentMethod = 'Credit card (automatic)' WHERE customerID = %s AND PaymentMethod NOT LIKE '%%(automatic)%%';", (user_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return {'event_type': 'enabled_autopay', 'user_id': user_id, 'details': 'Switched to Credit card (automatic)'}
    return None

# --- NEUTRAL EVENT HANDLER ---

def _handle_tenure_increase(conn, user_id):
    """Simulates a monthly anniversary for a user."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("UPDATE users SET tenure = tenure + 1, TotalCharges = TotalCharges + MonthlyCharges WHERE customerID = %s;", (user_id,))
        conn.commit()
        return {'event_type': 'monthly_anniversary', 'user_id': user_id, 'details': 'Tenure increased by 1 month'}
    return None

def run_simulator(producer, topic_name, user_ids):
    """The main simulation loop."""
    db_conn = get_db_connection()
    if not db_conn:
        raise Exception("Simulator could not connect to database.")
        
    print("Starting advanced event simulation...")
    while True:
        try:
            user_id = random.choice(user_ids)
            event = simulate_event(db_conn, user_id)
            
            if event:
                event['timestamp'] = time.time()
                print(f"SIMULATOR: Generating event -> {event}")
                producer.send(topic_name, value=event)
                producer.flush()
                
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            print(f"Database connection lost: {e}. Reconnecting...")
            if db_conn: db_conn.close()
            db_conn = get_db_connection()
        except Exception as e:
            print(f"An error occurred in the simulation loop: {e}")

        time.sleep(random.uniform(3, 7)) # Simulate events every 3-7 seconds

if __name__ == "__main__":
    AIVEN_SERVICE_URI = os.getenv("AIVEN_SERVICE_URI")
    KAFKA_TOPIC = "user_events_topic"
    
    conn = get_db_connection()
    if not conn: raise Exception("Initial database connection failed.")
    
    valid_user_ids = get_user_ids_from_db(conn)
    conn.close() # Close connection after getting IDs

    if not valid_user_ids:
        raise Exception("No user IDs found. Run db_setup.py first.")
        
    kafka_producer = create_kafka_producer(AIVEN_SERVICE_URI)
    
    try:
        run_simulator(kafka_producer, KAFKA_TOPIC, valid_user_ids)
    except KeyboardInterrupt:
        print("\nSimulator shutting down.")


