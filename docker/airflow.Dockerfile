FROM apache/airflow:2.10.5-python3.11
# Keep Airflow pinned while installing the Kubernetes provider on top of the base image.
RUN pip install --no-cache-dir \
    'apache-airflow==2.10.5' \
    'apache-airflow-providers-cncf-kubernetes==10.8.2' && \
    pip check
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
