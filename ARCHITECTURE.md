# Architecture Overview

This project uses a modern, event-driven architecture to process data in real-time and push updates to the user interface. The system is composed of several decoupled services that communicate through a central data stream and a backend API.

### High-Level Data Flow

```
[Kafka Topic] -> [Python Stream Processor] -> [PostgreSQL]
      |                                           |
      `----------------> [FastAPI Backend] <-------`
                                |
                          [WebSocket]
                                |
                         [React Frontend]
```

---

### Component Breakdown

#### 1. Data Source (Kafka Stream)
- **Role:** Acts as the central, durable log for all incoming user events (e.g., `contract_downgrade`, `added_tech_support`).
- **Function:** Decouples the event producers from the consumers. Any part of the system can generate an event without needing to know which service will process it.

#### 2. Stream Processor (`process_stream.py`)
- **Role:** The "brains" of the real-time prediction pipeline. It is a long-running Kafka consumer.
- **Workflow:**
    1.  **Consume:** Listens for and consumes new messages from the Kafka topic.
    2.  **Enrich:** For each event, it fetches the customer's complete, up-to-date feature set from the **PostgreSQL** database.
    3.  **Predict:** It uses a pre-trained **XGBoost** model to calculate a new churn probability score based on the enriched data.
    4.  **Log:** It saves the new prediction to the `predictions` table and the raw event to the `events` table for historical tracking.
    5.  **Notify:** It sends the event data along with its new churn score to the FastAPI backend via a `POST` request to the `/api/broadcast-event` endpoint.

#### 3. Database (PostgreSQL)
- **Role:** The persistent storage layer and the "source of truth" for customer state.
- **Key Tables:**
    - `users`: Acts as the primary feature store for customer attributes.
    - `predictions`: A historical log of all churn scores calculated over time for every user.
    - `events`: A raw log of all incoming events from the Kafka stream.
    - `intervention_log`: A record of manual actions taken by support staff.
    - `shap_summary`: Stores pre-calculated SHAP values to explain model predictions, updated by an offline script (`calculate_shap.py`).

#### 4. Backend API (FastAPI - `main.py`)
- **Role:** Serves as the primary interface between the frontend and the data. It has a dual responsibility.
- **Responsibilities:**
    1.  **REST API Server:** Provides standard HTTP endpoints for the frontend to fetch aggregated or historical data (e.g., `GET /api/dashboard-kpis`, `GET /api/customer/{id}`).
    2.  **WebSocket Server:** Manages persistent WebSocket connections from all active frontend clients. The `/ws/updates` endpoint handles connecting and disconnecting clients. The `/api/broadcast-event` endpoint receives data from the stream processor and pushes it to all connected clients via the `ConnectionManager`.

#### 5. Frontend (React)
- **Role:** The presentation layer responsible for all visualization and user interaction.
- **Workflow:**
    1.  **Initial Load:** When a page loads, it makes `fetch` requests to the FastAPI REST endpoints to get the initial state of the data.
    2.  **Live Connection:** It establishes a WebSocket connection using the custom `useWebSocket` hook.
    3.  **Real-Time Updates:** The hook listens for incoming messages broadcast by the server. When a new event arrives (e.g., a `churn_alert`), it updates the React state.
    4.  **Reactive Re-rendering:** The change in state automatically triggers React to re-render the necessary components (KPI cards, charts, live feed items), ensuring the UI always reflects the latest data without needing a page refresh.