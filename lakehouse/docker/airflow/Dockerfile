FROM apache/airflow:2.9.1-python3.10

USER root
RUN mkdir -p /opt/airflow/dags/repo && chown -R airflow: /opt/airflow/dags
COPY docker/airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
USER airflow
ENV PYTHONPATH="/opt/airflow/airflow_contrib:${PYTHONPATH}"
COPY docker/airflow/dags /opt/airflow/dags/repo
COPY airflow_contrib /opt/airflow/airflow_contrib
