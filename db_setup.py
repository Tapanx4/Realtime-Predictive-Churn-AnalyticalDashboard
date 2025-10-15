import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def setup_database():
    """
    Connects to the Supabase PostgreSQL database, creates/recreates all tables,
    and populates the 'users' table from the Kaggle dataset.
    """
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    if not POSTGRES_URI:
        raise ValueError("POSTGRES_URI not found in .env file.")

    conn = None
    try:
        print("Connecting to the Supabase database...")
        conn = psycopg2.connect(POSTGRES_URI)
        cursor = conn.cursor()
        print("Connection successful.")

        print("Creating tables: users, events, predictions, and intervention_log...")
        # Drop tables in reverse order of dependency
        cursor.execute("DROP TABLE IF EXISTS intervention_log, events, predictions, users;")

        cursor.execute("""
            CREATE TABLE users (
                customerID VARCHAR(255) PRIMARY KEY,
                gender VARCHAR(10), SeniorCitizen INT, Partner VARCHAR(3), Dependents VARCHAR(3),
                tenure INT, PhoneService VARCHAR(3), MultipleLines VARCHAR(20),
                InternetService VARCHAR(20), OnlineSecurity VARCHAR(20), OnlineBackup VARCHAR(20),
                DeviceProtection VARCHAR(20), TechSupport VARCHAR(20), StreamingTV VARCHAR(20),
                StreamingMovies VARCHAR(20), Contract VARCHAR(20), PaperlessBilling VARCHAR(3),
                PaymentMethod VARCHAR(50), MonthlyCharges FLOAT, TotalCharges FLOAT
            );

            CREATE TABLE events (
                event_id SERIAL PRIMARY KEY, user_id VARCHAR(255), event_type VARCHAR(255),
                event_timestamp TIMESTAMPTZ, event_data JSONB
            );

            CREATE TABLE predictions (
                prediction_id SERIAL PRIMARY KEY, user_id VARCHAR(255),
                churn_probability FLOAT, prediction_timestamp TIMESTAMPTZ
            );

            CREATE TABLE intervention_log (
                log_id SERIAL PRIMARY KEY,
                customer_id VARCHAR(255) NOT NULL,
                action_taken TEXT NOT NULL,
                log_timestamp TIMESTAMPTZ DEFAULT NOW(),
                agent_id VARCHAR(255) DEFAULT 'System'
            );
        """)
        conn.commit()
        print("Tables created successfully.")

        print("Populating 'users' table...")
        data_path = 'ml_model/WA_Fn-UseC_-Telco-Customer-Churn.csv'
        df = pd.read_csv(data_path)
        
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce').fillna(0.0)
        df_cleaned = df.drop('Churn', axis=1)

        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row)).decode('utf-8') for index, row in df_cleaned.iterrows()]
        
        cursor.execute("INSERT INTO users VALUES " + ",".join(values))
        
        conn.commit()
        print(f"Successfully inserted {len(df)} records into the 'users' table.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    setup_database()

