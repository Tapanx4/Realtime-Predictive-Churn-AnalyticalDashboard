# ChurnPredictionDashboard


An end-to-end, full-stack application designed to predict and visualize customer churn in real-time. This platform leverages a machine learning model and a live data pipeline to provide proactive, actionable insights, empowering businesses to identify and retain at-risk customers before they leave.

For a detailed explanation of the system design, please see the [Architecture Overview](ARCHITECTURE.md).

---

## Key Features

- **Real-Time KPI Dashboard**: A high-level overview of key metrics, including total active customers, number of high-risk customers, and MRR at risk. The dashboard automatically refreshes via WebSockets when significant events occur.
- **Live Event Feed**: A global, real-time stream of all user events. High-risk churn alerts are visually highlighted with a pulsing glow and distinct iconography to catch immediate attention.
- **Advanced Analytics Page**:
    - **Customer Flow Analysis**: A Sankey diagram visualizing the journey of customers from their lifecycle stage to their final predicted risk category.
    - **Model Explainability**: A SHAP bar chart that displays the top predictors influencing the machine learning model's decisions.
    - **Lifecycle Risk Breakdown**: Proportional bar charts showing the distribution of risk levels across different customer lifecycle stages.
- **Customer Watchlist**: A filterable, searchable, and paginated list of all customers, prioritized by their churn risk score.
- **Individual Customer Profiles**: A detailed drill-down page for each customer, showing their details, recent event history, and an interactive intervention log.
- **Persistent Light/Dark Mode**: A theme toggle that saves the user's preference in their browser.

---

## Technology Stack

| Category           | Technology                                                    |
| :----------------- | :------------------------------------------------------------ |
| **Frontend** | React, React Router, Tailwind CSS, Recharts, React Google Charts |
| **Backend** | Python, FastAPI, Uvicorn, WebSockets                          |
| **Data Pipeline** | Apache Kafka (`kafka-python`)                                 |
| **Database** | PostgreSQL                                                    |
| **Machine Learning**| Scikit-learn, XGBoost, SHAP, Pandas                           |

---

## Getting Started

### Prerequisites

- Node.js & npm
- Python 3.9+ & pip
- A running PostgreSQL instance
- A running Apache Kafka instance

### Installation & Setup

**1. Clone the Repository**
```bash
git clone <your-repository-url>
cd ChurnPredictionDashboard
```

**2. Backend Setup**
```bash
# Navigate to the backend directory
cd backend

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt

# Create a .env file by copying the example
cp ../.env.example ../.env 

# !! IMPORTANT !!
# Edit the new .env file in the root directory and add your actual 
# POSTGRES_URI and Kafka connection details.

# Run the FastAPI server
uvicorn main:app --reload
```
The backend will be running on `http://localhost:8000`.

**3. Frontend Setup**
```bash
# Open a new terminal and navigate to the frontend directory
cd frontend

# Install dependencies
npm install

# Run the React development server
npm start
```
The frontend will be running on `http://localhost:3000`.

**4. Stream Processor Setup**
This script listens to Kafka and powers the real-time updates.
```bash
# Open a third terminal and navigate to your stream processor's directory
cd path/to/your/processor/script

# Make sure you have an active virtual environment with dependencies installed

# Run the processor script
python process_stream.py 
```

**5. Machine Learning Model Training**
To generate the `shap_summary.json` file used by the Analytics page, run the calculation script.
```bash
# In the backend directory with your virtual environment active
python calculate_shap.py
```
This only needs to be run once to generate the file, or periodically to update the model explanations.