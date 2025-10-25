import asyncio
from contextlib import asynccontextmanager
import json
import joblib
import os
from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional

# Import the functions from your newly created processor.py file
from processor import (
    create_kafka_consumer,
    process_stream,
    get_db_connection
)

# Load environment variables from the root .env file
load_dotenv(dotenv_path='../.env')

# --- Background Task for Kafka Consumer ---
async def run_kafka_consumer_task():
    """
    A wrapper function to run the blocking Kafka consumer in the background.
    """
    print("BACKGROUND_TASK: Initializing...")
    
    # Load model and DB connection for the consumer
    model_path = os.getenv("MODEL_PATH", "../ml_model/churn_model_xgb.pkl")
    model = joblib.load(model_path)
    db_conn = get_db_connection()
    
    KAFKA_URI = os.getenv("AIVEN_KAFKA_URI")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events_topic")

    if not all([KAFKA_URI, KAFKA_TOPIC, model, db_conn]):
        print("BACKGROUND_TASK: ERROR - Missing configuration. Consumer will not start.")
        return

    consumer = create_kafka_consumer(KAFKA_URI, KAFKA_TOPIC)
    
    loop = asyncio.get_running_loop()
    print("BACKGROUND_TASK: Starting process_stream in a separate thread.")
    await loop.run_in_executor(None, process_stream, consumer, model, db_conn)

# --- FastAPI Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This code runs on application startup
    print("API_LIFESPAN: Startup detected. Starting Kafka consumer as a background task...")
    asyncio.create_task(run_kafka_consumer_task())
    yield
    # This code runs on application shutdown
    print("API_LIFESPAN: Shutdown detected.")

# --- Initialize FastAPI App ---
app = FastAPI(lifespan=lifespan)
origins = [
    "https://realtime-predictive-churn-analytica.vercel.app"
    "http://localhost:3000", # It's good practice to keep this for local development
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # In production, restrict this to your Vercel URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- WebSocket Connection Manager ---
class ConnectionManager:
    # ... (Your ConnectionManager class code)
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()
from fastapi import FastAPI, Response 

# ... (keep all your other code)

# --- NEW: Add this diagnostic endpoint ---
@app.get("/api/test-cors")
async def test_cors_endpoint():
    """
    A simple endpoint to manually return a CORS header for testing.
    """
    content = {"message": "CORS test successful"}
    
    # Manually create a response and add the header
    response = Response(content=json.dumps(content), media_type="application/json")
    response.headers["Access-Control-Allow-Origin"] = "*"
    
    print("--- Fired /api/test-cors endpoint with manual header ---")
    return response
    
@app.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("A client disconnected.")

@app.post("/api/broadcast-event")
async def broadcast_event(data: dict):
    """
    This endpoint receives any event data (from the Kafka processor)
    and broadcasts it to all connected WebSocket clients.
    """
    await manager.broadcast(json.dumps(data))
    return {"status": "broadcast successful"}

@app.post("/api/update-churn-risk")
async def update_churn_risk(data: dict):
    await manager.broadcast(json.dumps(data))
    return {"status": "success", "data_broadcasted": data}

from psycopg2.extras import RealDictCursor

@app.get("/api/dashboard-kpis")
async def get_dashboard_kpis():
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 1. Risk categories
            cursor.execute("""
                SELECT
    -- Updated logic for "Critical"
    COUNT(CASE WHEN p.churn_probability >= 0.85 THEN 1 END) AS critical_risk_customers,

    -- Updated logic for "High"
    COUNT(CASE WHEN p.churn_probability > 0.70 AND p.churn_probability < 0.85 THEN 1 END) AS high_risk_customers,

    -- Updated logic for "Medium"
    COUNT(CASE WHEN p.churn_probability > 0.40 AND p.churn_probability <= 0.70 THEN 1 END) AS medium_risk_customers,

    -- Updated logic for "Low"
    COUNT(CASE WHEN p.churn_probability <= 0.40 THEN 1 END) AS low_risk_customers
FROM (
    SELECT DISTINCT ON (user_id) user_id, churn_probability
    FROM predictions
    ORDER BY user_id, prediction_timestamp DESC
) p;
            """)
            risk_counts = cursor.fetchone()

            # 2. Total active customers
            cursor.execute("SELECT COUNT(*) FROM users;")
            total_active = cursor.fetchone()["count"]

            # 3. MRR at risk (only high + critical risk customers)
            cursor.execute("""
                SELECT SUM(u.MonthlyCharges) AS mrr_at_risk
                FROM users u
                JOIN (
                    SELECT DISTINCT ON (user_id) user_id, churn_probability
                    FROM predictions 
                    ORDER BY user_id, prediction_timestamp DESC
                ) p ON u.customerID = p.user_id
                WHERE p.churn_probability > 0.70;
            """)
            mrr_result = cursor.fetchone()
            mrr_at_risk = mrr_result["mrr_at_risk"] or 0

            # 4. Churn probability trend (last 24h, hourly average)
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('hour', prediction_timestamp) AS hour,
                    AVG(churn_probability) AS avg_prob
                FROM predictions
                WHERE prediction_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY hour
                ORDER BY hour;
            """)
            churn_trend = cursor.fetchall()

            # Calculate overall average churn probability for the last 24h
            # If no data in last 24h, fallback to latest distinct churn_probability per user
            if churn_trend:
                # average of all avg_prob in churn_trend for 24h
                avg_probs = [row["avg_prob"] for row in churn_trend if row["avg_prob"] is not None]
                overall_avg_24h = sum(avg_probs) / len(avg_probs) if avg_probs else None
            else:
                overall_avg_24h = None

            if overall_avg_24h is None:
                # fallback to average latest churn_probability of all users
                cursor.execute("""
                    SELECT AVG(p.churn_probability) AS avg_prob
                    FROM (
                        SELECT DISTINCT ON (user_id) user_id, churn_probability
                        FROM predictions
                        ORDER BY user_id, prediction_timestamp DESC
                    ) p;
                """)
                fallback_result = cursor.fetchone()
                overall_avg_24h = fallback_result["avg_prob"] or 0.0

            # 5. Churn by segment (high-risk only)
            cursor.execute("""
                SELECT 
                    u.contract,
                    COUNT(*) AS count
                FROM users u
                JOIN (
                    SELECT DISTINCT ON (user_id) user_id, churn_probability
                    FROM predictions 
                    ORDER BY user_id, prediction_timestamp DESC
                ) p ON u.customerID = p.user_id
                WHERE p.churn_probability > 0.70
                GROUP BY u.contract;
            """)
            churn_by_segment = cursor.fetchall()

        return {
            **risk_counts,
            "total_active_customers": total_active,
            "mrr_at_risk": float(mrr_at_risk),
            "churn_trend": churn_trend,
            "churn_by_segment": churn_by_segment,
            "overall_avg_churn_probability": float(overall_avg_24h)
        }

    finally:
        if conn:
            conn.close()

# --- NEW: /api/watchlist Endpoint ---
@app.get("/api/watchlist")
async def get_watchlist(
    page: int = 1,
    search: str = "",
    risk: Optional[List[str]] = Query(None),
    contract: Optional[List[str]] = Query(None)
):
    conn = get_db_connection()
    if not conn: return {"error": "Database connection failed"}
    
    limit = 10
    offset = (page - 1) * limit
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            base_query = """
                FROM users u
                JOIN (
                    SELECT DISTINCT ON (user_id) user_id, churn_probability
                    FROM predictions ORDER BY user_id, prediction_timestamp DESC
                ) p ON u.customerid = p.user_id
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, event_timestamp
                    FROM events ORDER BY user_id, event_timestamp DESC
                ) e ON u.customerid = e.user_id
            """
            
            where_clauses = ["u.customerID ILIKE %(search)s"]
            params = {"search": f"%{search}%", "limit": limit, "offset": offset}

            if contract:
                where_clauses.append("u.contract = ANY(%(contract)s)")
                params["contract"] = contract
                
            if risk:
                risk_conditions = []
                if 'Critical' in risk: risk_conditions.append("p.churn_probability >= 0.85")
                if 'High' in risk: risk_conditions.append("(p.churn_probability > 0.70 AND p.churn_probability < 0.85)")
                if 'Medium' in risk: risk_conditions.append("(p.churn_probability > 0.40 AND p.churn_probability <= 0.70)")
                if 'Low' in risk: risk_conditions.append("p.churn_probability <= 0.40")
                if risk_conditions:
                     where_clauses.append(f"({' OR '.join(risk_conditions)})")

            full_where_clause = " WHERE " + " AND ".join(where_clauses)
            
            cursor.execute(f"SELECT COUNT(*) {base_query} {full_where_clause}", params)
            total_users = cursor.fetchone()['count']
            
            cursor.execute(f"SELECT u.*, p.churn_probability as risk_score, e.event_timestamp as last_activity {base_query} {full_where_clause} ORDER BY risk_score DESC LIMIT %(limit)s OFFSET %(offset)s", params)
            users = cursor.fetchall()

        return {"users": users, "total_pages": (total_users + limit - 1) // limit if total_users else 0}
    finally:
        if conn: conn.close()

@app.get("/api/customer/{customer_id}")
async def get_customer_profile(customer_id: str):
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get user details (no change)
            cursor.execute("SELECT * FROM users WHERE customerID = %s;", (customer_id,))
            user_details = cursor.fetchone()

            # Get latest prediction (no change)
            cursor.execute("""
                SELECT * FROM predictions 
                WHERE user_id = %s 
                ORDER BY prediction_timestamp DESC 
                LIMIT 1;
            """, (customer_id,))
            prediction = cursor.fetchone()

            # Get intervention log (no change)
            cursor.execute("""
                SELECT * FROM intervention_log 
                WHERE customer_id = %s 
                ORDER BY log_timestamp DESC;
            """, (customer_id,))
            intervention_log = cursor.fetchall()

            # --- NEW: Get the 15 most recent events for this user ---
            cursor.execute("""
                SELECT * FROM events 
                WHERE user_id = %s 
                ORDER BY event_timestamp DESC
                LIMIT 15;
            """, (customer_id,))
            recent_events = cursor.fetchall()

        # Merge user details and prediction safely
        if not user_details:
            # --- MODIFIED: Also return recent_events in this case ---
            return {
                "details": None, 
                "intervention_log": intervention_log,
                "recent_events": recent_events
            }

        # Combine both dicts if prediction exists
        if prediction:
            user_details.update({
                "churn_probability": prediction["churn_probability"],
                "prediction_timestamp": prediction["prediction_timestamp"]
            })

        # --- MODIFIED: Add recent_events to the final return object ---
        return {
            "details": user_details,
            "intervention_log": intervention_log,
            "recent_events": recent_events
        }

    finally:
        if conn:
            conn.close()

@app.post("/api/customer/{customer_id}/log-intervention")
async def log_intervention(customer_id: str, data: dict):
    action = data.get("description")  # <-- Fix here

    if not action:
        return {"error": "Action taken cannot be null or empty"}

    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO intervention_log (customer_id, action_taken) VALUES (%s, %s)",
                (customer_id, action)
            )
            conn.commit()
        return {"status": "success", "log": f"Action '{action}' logged for customer {customer_id}."}
    finally:
        if conn:
            conn.close()
# GET: Churn alert history for frontend
@app.get("/api/churn-alerts-history")
async def get_churn_alerts_history(limit: int = 7048):
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    p.user_id AS customer_id,
                    p.churn_probability,
                    u.contract AS contract_type,
                    u.tenure,
                    u.monthlycharges AS monthly_charges,
                    u.totalcharges AS total_charges,
                    u.techsupport,
                    p.prediction_timestamp AS id
                FROM predictions p
                JOIN users u ON u.customerid = p.user_id
                ORDER BY p.prediction_timestamp DESC
                LIMIT %s;
            """, (limit,))
            rows = cursor.fetchall()

            # Add churn_risk
            enriched = []
            for row in rows:
                prob = row["churn_probability"]
                if prob >= 0.85:
                    row["churn_risk"] = "Critical"
                elif prob > 0.70:
                    row["churn_risk"] = "High"
                elif prob > 0.40:
                    row["churn_risk"] = "Medium"
                else:
                    row["churn_risk"] = "Low"
                enriched.append(row)

        return enriched
    finally:
        conn.close()
import json
@app.get("/api/shap-summary")
async def get_shap_summary():
    """
    Fetches the pre-calculated SHAP feature importances from a JSON file.
    """
    try:
        # Open and read the JSON file created by the offline script
        with open("shap_summary.json", "r") as f:
            shap_values = json.load(f)
        return shap_values
    except FileNotFoundError:
        # This error occurs if the calculation script has not been run yet
        raise HTTPException(
            status_code=404, 
            detail="SHAP summary file not found. Please run the 'calculate_shap.py' script first."
        )
    except Exception as e:
        # Catch other potential errors like corrupted JSON
        raise HTTPException(
            status_code=500, 
            detail=f"An error occurred while reading SHAP data: {str(e)}"
        )
# Add this endpoint to your main.py file

@app.get("/api/events/history")
async def get_events_history(limit: int = 50):
    """
    Provides a list of the most recent events logged in the database.
    """
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Fetch the most recent events from the events table
            cursor.execute("""
                SELECT * FROM events 
                ORDER BY event_timestamp DESC 
                LIMIT %s;
            """, (limit,))
            events = cursor.fetchall()
            return events
    except Exception as e:
        print(f"Error fetching event history: {e}")
        return {"error": "Could not fetch event history."}
    finally:
        if conn:
            conn.close()
            
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

