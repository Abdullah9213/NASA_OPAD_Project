

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


# --- Helper Function for Git ---
def run_git_command(command, check=True):
    """Helper to run a shell command from the project root."""
    try:
        subprocess.run(
            command,
            check=check,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {e.stderr}")
        raise

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
    5. Version (DVC): Initialize Git & DVC, then run 'dvc add'.
    6. Version (Git): Commit and push the .dvc metadata file to GitHub.
    """

    # --- Tasks 1-3 (Unchanged) ---
    @task
    def extract_apod_data():
        print("--- Task 1: Fetching data from NASA API ---")
        response = requests.get(NASA_API_URL)
        response.raise_for_status()
        return response.json()

    @task
    def transform_apod_data(raw_data: dict) -> pd.DataFrame:
        print("--- Task 2: Transforming data into DataFrame ---")
        selected_data = {field: [raw_data.get(field)] for field in FIELDS_OF_INTEREST}
        df = pd.DataFrame(selected_data)
        df['date'] = pd.to_datetime(df['date'])
        return df

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

    @task
    def load_to_csv(df: pd.DataFrame):
        print("--- Task 3b: Saving data to CSV ---")
        os.makedirs(CSV_DIR, exist_ok=True)
        df.to_csv(CSV_PATH, index=False)
        print(f"Successfully saved data to {CSV_PATH}")
        return CSV_RELATIVE_PATH

    # --- Task 4: Data Versioning (DVC) ---
    @task
    def version_data_with_dvc(relative_csv_path: str):
        """
        Initializes the Git repo inside the container, pulls DVC config,
        and then runs 'dvc add' on the CSV file.
        """
        print("--- Task 4: Initializing Git Repo and Versioning data with DVC ---")
        
        # Get GitHub credentials
        github_pat = Variable.get("GITHUB_PAT")
        github_user = Variable.get("GITHUB_USER")
        github_repo = Variable.get("GITHUB_REPO_URL")
        push_url = f"https://{github_user}:{github_pat}@{github_repo}"

        # 1. Check if .git exists. If not, initialize the repo.
        if not os.path.exists(os.path.join(PROJECT_ROOT, ".git")):
            print("No .git directory found. Initializing Git repo...")
            run_git_command(['git', 'init', '-b', 'main'])
            run_git_command(['git', 'config', '--global', 'user.email', 'airflow@example.com'])
            run_git_command(['git', 'config', '--global', 'user.name', 'Airflow-Bot'])
            run_git_command(['git', 'remote', 'add', 'origin', push_url])
            
            # Pull the .dvc folder from the repo
            try:
                # We use pull --allow-unrelated-histories in case the init created a different root
                run_git_command(['git', 'pull', 'origin', 'main', '--allow-unrelated-histories'])
                print("Successfully pulled .dvc config from repo.")
            except subprocess.CalledProcessError as e:
                print(f"Git pull failed (maybe new repo?): {e.stderr}")
                # We need to run dvc init if the pull failed to bring it
                if not os.path.exists(os.path.join(PROJECT_ROOT, ".dvc")):
                    print("No .dvc folder. Running 'dvc init'...")
                    run_git_command(['dvc', 'init'])
        
        # 2. Now, run 'dvc add'
        print(f"Running 'dvc add {relative_csv_path}'")
        run_git_command(['dvc', 'add', relative_csv_path])
        print("Successfully ran 'dvc add'")
        return DVC_FILE_PATH

    # --- Task 5: Code Versioning (Git/GitHub) ---
    @task
    def version_code_with_git(dvc_file: str):
        """
        Commits and pushes the updated .dvc metadata file to GitHub.
        """
        print("--- Task 5: Committing .dvc file to Git ---")
        
        # Get GitHub credentials
        github_pat = Variable.get("GITHUB_PAT")
        github_user = Variable.get("GITHUB_USER")
        github_repo = Variable.get("GITHUB_REPO_URL")
        push_url = f"https://{github_user}:{github_pat}@{github_repo}"

        # Git config should be set by the previous task, but we check
        run_git_command(['git', 'config', '--global', 'user.email', 'airflow@example.com'], check=False)
        run_git_command(['git', 'config', '--global', 'user.name', 'Airflow-Bot'], check=False)

        # 1. Add the .dvc file
        run_git_command(['git', 'add', dvc_file])
        
        # 2. Commit
        commit_message = f"Data: Update APOD data for {pendulum.today().to_date_string()}"
        # Check if there's anything to commit
        status_result = subprocess.run(['git', 'status', '--porcelain'], cwd=PROJECT_ROOT, capture_output=True, text=True)
        if dvc_file not in status_result.stdout:
            print("No data changes to commit.")
            return

        run_git_command(['git', 'commit', '-m', commit_message])
        
        # 3. Push to GitHub
        run_git_command(['git', 'push', push_url, 'main'])
        print(f"Successfully committed and pushed {dvc_file} to GitHub.")

    # --- Define Task Dependencies ---
    raw_data = extract_apod_data()
    clean_df = transform_apod_data(raw_data)
    
    pg_load_task = load_to_postgres(clean_df)
    csv_path_task = load_to_csv(clean_df)

    dvc_file_task = version_data_with_dvc(csv_path_task)
    
    git_task = version_code_with_git(dvc_file_task)
    git_task.set_upstream(pg_load_task)


# Instantiate the DAG
apod_pipeline()