import pandas as pd
import shap
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
import json

def calculate_and_store_shap():
    """
    Main function to fetch data from a CSV file, train a model, calculate SHAP values,
    and store the aggregated results in a JSON file.
    """
    try:
        # --- MODIFIED STEP 1: Read data from a local CSV file ---
        # IMPORTANT: Replace with the actual path and name of your CSV file.
        # The path '../ml_model/' assumes the ml_model folder is outside your backend folder.
        csv_path = 'E:\churn-dashboard\ml_model\WA_Fn-UseC_-Telco-Customer-Churn.csv' 
        print(f"Reading user data from {csv_path}...")
        df = pd.read_csv(csv_path)
        print(f"Successfully loaded {len(df)} user records from CSV.")
        
        # --- Data Cleaning (ensure column names match your CSV) ---
        # This assumes your CSV has a column named 'TotalCharges' and 'Churn'.
        # Please verify these names against your actual file.
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        df.dropna(inplace=True)
        # The column name 'Churn' might be different in your file.
        df['Churn_encoded'] = df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)

        # --- Steps 2-6: Model Training & SHAP Calculation (largely unchanged) ---
        features_to_use = [col for col in df.columns if col not in ['customerID', 'Churn', 'Churn_encoded']]
        X = df[features_to_use]
        y = df['Churn_encoded'] # Use the newly encoded target column

        categorical_features = X.select_dtypes(include=['object']).columns
        numerical_features = X.select_dtypes(include=['int64', 'float64']).columns

        preprocessor = ColumnTransformer(
            transformers=[
                ('num', 'passthrough', numerical_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ])

        X_processed = preprocessor.fit_transform(X)
        feature_names = list(numerical_features) + \
                        list(preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_features))

        print("Training XGBoost churn prediction model...")
        X_train, X_test, y_train, y_test = train_test_split(X_processed, y, test_size=0.2, random_state=42)
        model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
        model.fit(X_train, y_train)
        print("Model training complete.")

        print("Calculating SHAP values...")
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)
        
        shap_df = pd.DataFrame(abs(shap_values), columns=feature_names)
        mean_shap_values = shap_df.mean().sort_values(ascending=False)
        
        # --- Step 7: Store results in a JSON file ---
        print("Saving SHAP summary to shap_summary.json...")
        
        shap_results = [
            {"feature": feature, "importance": importance} 
            for feature, importance in mean_shap_values.head(10).items()
        ]

        # This will save the file in the same directory where you run the script (e.g., inside churn_backend)
        with open("shap_summary.json", "w") as f:
            json.dump(shap_results, f, indent=4)
            
        print("Successfully saved results to shap_summary.json.")

    except FileNotFoundError:
        print(f"ERROR: The file was not found at the specified path: {csv_path}")
        print("Please make sure the file exists and the path is correct.")
    except KeyError as e:
        print(f"ERROR: A column name was not found in the CSV file: {e}")
        print("Please check that the column names in the script match your CSV headers exactly.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    calculate_and_store_shap()