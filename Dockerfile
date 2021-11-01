FROM apache/airflow:2.2.0
RUN pip install --user psycopg2-binary
RUN pip install --user apache-airflow-providers-postgres
RUN pip install --user apache-airflow[slack]
RUN pip install --user requests
ENV AIRFLOW_HOME=/opt/airflow
ENV slack_webhook_url=