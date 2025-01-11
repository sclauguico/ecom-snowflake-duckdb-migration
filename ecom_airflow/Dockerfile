FROM quay.io/astronomer/astro-runtime:12.6.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

FROM --platform=linux/amd64 quay.io/astronomer/astro-runtime:8.6.0