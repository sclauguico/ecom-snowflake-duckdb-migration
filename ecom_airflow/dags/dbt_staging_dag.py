import os
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from datetime import datetime

ENV = os.getenv("DBT_ENV", "prod")
target_profile = "motherduck"
target_name = "prod" if ENV == "prod" else "dev"

# Define project directory
DBT_PROJECT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "dbt",
    "ecom_staging"
)

dbt_staging_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_DIR
    ),
    profile_config=ProfileConfig(
        profile_name=target_profile,
        target_name=target_name,
        profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml")
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_ecom_staging_dag",
    operator_args={
        "install_deps": True,
        "models": ["stg_*", "fct_*", "dim_*"],
        "full_refresh": True,
        "vars": {
            "is_test": ENV != "prod",
            "load_date": "{{ ds }}"
        }
    },
    tags=["dbt", "staging", "ecommerce"]
)