FROM apache/airflow:2.7.3-python3.10

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Change to root and update package manager
USER root
RUN apt-get update && apt-get clean 

# Change back to airflow
USER airflow
# Copy and install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Copy project dirs
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root config/ /opt/airflow/config/
# Create output dirs
RUN mkdir -p /opt/airflow/logs /opt/airflow/data
# Copy entrypoint script
COPY docker/entrypoint.sh /opt/airflow/entrypoint.sh

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]

WORKDIR /opt/airflow
