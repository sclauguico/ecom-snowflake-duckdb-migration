FROM quay.io/astronomer/astro-runtime:9.1.0

# Install dbt and related packages
RUN pip install --no-cache-dir dbt-core==1.6.1 dbt-duckdb==1.6.1

# Install Cosmos
RUN pip install 'astronomer-cosmos[duckdb]'

# Create virtual environment for dbt
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-core==1.6.1 dbt-duckdb==1.6.1

# Switch to root for permission-sensitive operations
USER root

# Make dbt executable available
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"
RUN ln -sf /usr/local/airflow/dbt_venv/bin/dbt /usr/local/bin/dbt

# Create directory for dbt and set permissions
RUN mkdir -p /usr/local/airflow/dbt
RUN chown -R astro:astro /usr/local/airflow/dbt_venv /usr/local/airflow/dbt /usr/local/bin/dbt

# Switch back to astro user
USER astro

# Copy dbt project files
COPY dags/dbt/ecom_staging/ /usr/local/airflow/dbt/ecom_staging/
COPY dags/dbt/ecom_intermediate/ /usr/local/airflow/dbt/ecom_intermediate/