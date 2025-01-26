from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

profile_config = ProfileConfig(
    profile_name="motherduck",
    target_name="prod",
    profiles_yml_filepath="/usr/local/airflow/dags/dbt/ecom_marts/profiles.yml"
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

marts_dag = DbtDag(
    dag_id="dbt_ecom_marts_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/ecom_marts",
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "vars": {
            "is_test": False,
            "load_date": "{{ ds }}"
        },
        "full_refresh": True,
    },
    max_active_runs=1,
    tags=['dbt', 'marts'],
)