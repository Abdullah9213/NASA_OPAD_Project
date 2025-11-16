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
        # We capture output to prevent secrets from being in the *main* log
        # but they might still appear if the command fails, as seen in previous logs.
        result = subprocess.run(
            command,
            check=check,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True
        )
        # Log stdout/stderr for debugging non-secret commands
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
    except subprocess.CalledProcessError as e:
        # Print stderr on failure for debugging, but be aware of secrets
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
    5. Version (Git/DVC): Version data with DVC and push .dvc file to Git.
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

    # --- Task 4 & 5 Combined: Version and Push ---
    @task
    def version_data_and_code(relative_csv_path: str):
        """
        Initializes Git, stashes local files, pulls, runs dvc add,
        commits, and pushes the .dvc file.
        This combines both versioning tasks to avoid Git conflicts.
        """
        print("--- Task 4/5: Versioning Data and Pushing to Git ---")
        
        # 1. Get GitHub credentials
        github_pat = Variable.get("GITHUB_PAT")
        github_user = Variable.get("GITHUB_USER")
        github_repo = Variable.get("GITHUB_REPO_URL")
        push_url = f"https://{github_user}:{github_pat}@{github_repo}"

        # 2. Initialize Git if needed
        if not os.path.exists(os.path.join(PROJECT_ROOT, ".git")):
            print("No .git directory found. Initializing Git repo...")
            run_git_command(['git', 'init', '-b', 'main'])
            run_git_command(['git', 'config', '--global', 'user.email', 'airflow@example.com'])
            run_git_command(['git', 'config', '--global', 'user.name', 'Airflow-Bot'])
            # Don't add the remote with the PAT here, we set it below
            run_git_command(['git', 'remote', 'add', 'origin', f"https://{github_repo}"])
        
        # 3. Set remote URL (in case it's wrong) and STASH
        # This is the fix for the "untracked files" error
        print("Setting remote URL and stashing local files...")
        run_git_command(['git', 'remote', 'set-url', 'origin', push_url])
        run_git_command(['git', 'stash', '--include-untracked'], check=False)

        # 4. Pull latest changes
        print("Pulling latest changes from origin/main with rebase...")
        try:
            run_git_command(['git', 'pull', 'origin', 'main', '--rebase'])
            print("Successfully pulled and rebased.")
        except subprocess.CalledProcessError as e:
            if "couldn't find remote ref main" in str(e): # Check stderr in string
                print("Remote 'main' branch probably doesn't exist yet. Skipping pull.")
            else:
                print(f"Git pull failed: {e.stderr}")
                raise

        # 5. Run 'dvc add'
        print(f"Running 'dvc add {relative_csv_path}'")
        run_git_command(['dvc', 'add', relative_csv_path])
        print("Successfully ran 'dvc add'")
        
        # 6. Add and Commit the .dvc file
        print("Adding and committing .dvc file...")
        run_git_command(['git', 'add', DVC_FILE_PATH])
        
        # Check if there's anything to commit
        status_result = subprocess.run(['git', 'status', '--porcelain'], cwd=PROJECT_ROOT, capture_output=True, text=True)
        if DVC_FILE_PATH not in status_result.stdout:
            print("No data changes to commit.")
            return

        commit_message = f"Data: Update APOD data for {pendulum.today().to_date_string()}"
        run_git_command(['git', 'commit', '-m', commit_message])
        
        # 7. Push to GitHub
        print("Pushing commit to GitHub...")
        run_git_command(['git', 'push', 'origin', 'main'])
        print(f"Successfully committed and pushed {DVC_FILE_PATH} to GitHub.")


    # --- Define Task Dependencies ---
    raw_data = extract_apod_data()
    clean_df = transform_apod_data(raw_data)
    
    pg_load_task = load_to_postgres(clean_df)
    csv_path_task = load_to_csv(clean_df)

    # NEW: Call the single, combined task
    version_task = version_data_and_code(csv_path_task)
    
    # Make sure versioning only happens after the CSV and PG load are done
    version_task.set_upstream(pg_load_task)


# Instantiate the DAG
apod_pipeline()