

import pendulum
import requests
import pandas as pd
import os
import subprocess  # For running shell commands
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable  # To read our GitHub secrets

# --- Constants ---
NASA_API_URL = "https://api.nasa.gov/planetary/apod?api_key=WMlSqy6pZvvIYLbP4lQSmsPXzDUDNySV8HbLgYn6"
FIELDS_OF_INTEREST = ['date', 'title', 'url', 'explanation']

# The project root *inside the Docker container* is /usr/local/airflow
PROJECT_ROOT = "/usr/local/airflow"
CSV_DIR = f"{PROJECT_ROOT}/data"
CSV_PATH = f"{CSV_DIR}/apod_data.csv"
# DVC needs the path *relative* to the project root
CSV_RELATIVE_PATH = "data/apod_data.csv"
DVC_FILE_PATH = f"{CSV_RELATIVE_PATH}.dvc"


# --- DAG Definition ---
@dag(
    dag_id="nasa_apod_pipeline",
    start_date=pendulum.today('UTC'),
    schedule="@daily",
    catchup=False,
)
def apod_pipeline():
    """
    Full MLOps Pipeline:
    1. Extract: Fetch data from NASA APOD API.
    2. Transform: Clean and structure data into a Pandas DataFrame.
    3. Load (Postgres): Load DataFrame into a Postgres table.
    4. Load (CSV): Save DataFrame to a local CSV file.
    5. Version (DVC): Run 'dvc add' on the new CSV file.
    6. Version (Git): Commit and push the .dvc metadata file to GitHub.
    """

    # --- Task 1: Data Extraction (E) ---
    @task
    def extract_apod_data():
        print("--- Task 1: Fetching data from NASA API ---")
        response = requests.get(NASA_API_URL)
        response.raise_for_status()
        return response.json()

    # --- Task 2: Data Transformation (T) ---
    @task
    def transform_apod_data(raw_data: dict) -> pd.DataFrame:
        print("--- Task 2: Transforming data into DataFrame ---")
        selected_data = {field: [raw_data.get(field)] for field in FIELDS_OF_INTEREST}
        df = pd.DataFrame(selected_data)
        df['date'] = pd.to_datetime(df['date'])
        return df

    # --- Task 3a: Data Loading (L) - Postgres ---
    @task
    def load_to_postgres(df: pd.DataFrame):
        print("--- Task 3a: Loading data to Postgres ---")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("""
        CREATE TABLE IF NOT EXISTS apod_data (
            date DATE PRIMARY KEY,
            title TEXT,
            url TEXT,
            explanation TEXT
        );
        """)
        engine = hook.get_sqlalchemy_engine()
        df.to_sql('apod_data', con=engine, if_exists='replace', index=False)
        print("Successfully loaded data to Postgres.")

    # --- Task 3b: Data Loading (L) - CSV ---
    @task
    def load_to_csv(df: pd.DataFrame):
        print("--- Task 3b: Saving data to CSV ---")
        os.makedirs(CSV_DIR, exist_ok=True)
        df.to_csv(CSV_PATH, index=False)
        print(f"Successfully saved data to {CSV_PATH}")
        # Return the *relative* path for DVC
        return CSV_RELATIVE_PATH

    # --- Task 4: Data Versioning (DVC) ---
    @task
    def version_data_with_dvc(relative_csv_path: str):
        """
        Runs 'dvc add' on the CSV file.
        Uses subprocess to run shell commands from the project root.
        """
        print("--- Task 4: Versioning data with DVC ---")
        try:
            # Run 'dvc add'
            subprocess.run(
                ["dvc", "add", relative_csv_path],
                check=True,  # Fail the task if DVC fails
                cwd=PROJECT_ROOT,  # Run from the project root
                capture_output=True,
                text=True
            )
            print(f"Successfully ran 'dvc add {relative_csv_path}'")
            return DVC_FILE_PATH
        except subprocess.CalledProcessError as e:
            print(f"DVC add failed: {e.stderr}")
            raise

    # --- Task 5: Code Versioning (Git/GitHub) ---
    @task
    def version_code_with_git(dvc_file: str):
        """
        Commits and pushes the updated .dvc metadata file to GitHub.
        """
        print("--- Task 5: Committing .dvc file to Git ---")
        
        # Get GitHub credentials from Airflow Variables
        github_pat = Variable.get("GITHUB_PAT")
        github_user = Variable.get("GITHUB_USER")
        github_repo = Variable.get("GITHUB_REPO_URL")
        
        # Construct the authenticated Git URL
        push_url = f"https://{github_user}:{github_pat}@{github_repo}"

        try:
            # 1. Configure Git user
            subprocess.run(['git', 'config', '--global', 'user.email', 'airflow@example.com'], cwd=PROJECT_ROOT, check=True)
            subprocess.run(['git', 'config', '--global', 'user.name', 'Airflow-Bot'], cwd=PROJECT_ROOT, check=True)
            
            # 2. Add the .dvc file (NOT the .csv)
            subprocess.run(['git', 'add', dvc_file], cwd=PROJECT_ROOT, check=True)
            
            # 3. Commit
            commit_message = f"Data: Update APOD data for {pendulum.today().to_date_string()}"
            # Check if there's anything to commit
            status_result = subprocess.run(['git', 'status', '--porcelain'], cwd=PROJECT_ROOT, capture_output=True, text=True)
            if dvc_file not in status_result.stdout:
                print("No data changes to commit.")
                return

            subprocess.run(['git', 'commit', '-m', commit_message], cwd=PROJECT_ROOT, check=True)
            
            # 4. Push to GitHub
            subprocess.run(['git', 'push', push_url, 'main'], cwd=PROJECT_ROOT, check=True)
            print(f"Successfully committed and pushed {dvc_file} to GitHub.")
            
        except subprocess.CalledProcessError as e:
            print(f"Git operation failed: {e.stderr}")
            raise

    # --- Define Task Dependencies ---
    raw_data = extract_apod_data()
    clean_df = transform_apod_data(raw_data)
    
    # Load tasks
    pg_load_task = load_to_postgres(clean_df)
    csv_path_task = load_to_csv(clean_df) # This task returns the path

    # Versioning tasks
    dvc_file_task = version_data_with_dvc(csv_path_task)
    
    # Final Git task
    # It depends on both the Postgres and DVC tasks succeeding
    git_task = version_code_with_git(dvc_file_task)
    git_task.set_upstream(pg_load_task)


# Instantiate the DAG
apod_pipeline()