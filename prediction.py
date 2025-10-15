import os
import pandas as pd
import joblib
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from the .env file in the root directory
load_dotenv()

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

def backfill_initial_predictions():
    """
    Loads the original dataset, calculates churn probability for each user
    using the trained model, and inserts these initial predictions into the
    'predictions' table.
    """
    print("Starting initial predictions backfill process...")

    MODEL_PATH = 'ml_model/churn_model_xgb.pkl'
    DATA_PATH = 'ml_model/WA_Fn-UseC_-Telco-Customer-Churn.csv'

    # 1. Load the trained model
    try:
        model = joblib.load(MODEL_PATH)
        print("Successfully loaded XGBoost model.")
    except FileNotFoundError:
        raise Exception(f"Model file not found at {MODEL_PATH}. Please run train_model.py first.")

    # 2. Load and prepare the original dataset
    try:
        df = pd.read_csv(DATA_PATH)
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce').fillna(0.0)
        # The data to predict on should not include the target variable or the ID
        X = df.drop(['Churn'], axis=1)
        print("Successfully loaded and prepared dataset.")
    except FileNotFoundError:
        raise Exception(f"Dataset file not found at {DATA_PATH}.")

    # 3. Calculate churn probabilities for the entire dataset
    print("Calculating initial churn probabilities for all users...")
    # The second column [:, 1] is the probability of the "positive" class (Churn='Yes')
    probabilities = model.predict_proba(X.drop('customerID', axis=1))[:, 1]
    
    # 4. Prepare data for database insertion
    customer_ids = X['customerID'].tolist()
    timestamp = datetime.now()
    
    # Create a list of tuples to insert
    predictions_data = [
        (customer_ids[i], float(probabilities[i]), timestamp)
        for i in range(len(customer_ids))
    ]
    print(f"Generated {len(predictions_data)} predictions.")

    # 5. Connect to the database and insert predictions
    conn = get_db_connection()
    if not conn:
        raise Exception("Could not connect to the database.")
    
    try:
        with conn.cursor() as cursor:
            print("Inserting initial predictions into the database...")
            # Use execute_values for efficient bulk insertion
            execute_values(
                cursor,
                "INSERT INTO predictions (user_id, churn_probability, prediction_timestamp) VALUES %s",
                predictions_data
            )
            conn.commit()
            print(f"Successfully inserted {len(predictions_data)} initial predictions.")
    except Exception as e:
        print(f"An error occurred during database insertion: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    backfill_initial_predictions()

    
