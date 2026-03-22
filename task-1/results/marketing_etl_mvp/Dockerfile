FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv to /usr/local/bin (system-wide)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

USER airflow

# Copy project files
COPY --chown=airflow:root pyproject.toml /opt/airflow/
COPY --chown=airflow:root uv.lock /opt/airflow/
COPY --chown=airflow:root .python-version /opt/airflow/
COPY --chown=airflow:root src /opt/airflow/src/
COPY --chown=airflow:root dags /opt/airflow/dags/

# Install project dependencies into a dedicated project virtualenv.
WORKDIR /opt/airflow
ENV UV_PROJECT_ENVIRONMENT=/opt/airflow/.venv
ENV PATH="/opt/airflow/.venv/bin:${PATH}"
RUN uv sync --no-dev

# Create data directory
USER root
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data
USER airflow

WORKDIR /opt/airflow
