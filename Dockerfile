FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:airflow extraccion/ /opt/airflow/extraccion/
COPY --chown=airflow:airflow transformacion/ /opt/airflow/transformacion/
COPY --chown=airflow:airflow carga/ /opt/airflow/carga/
COPY --chown=airflow:airflow dags/ /opt/airflow/dags/
