FROM apache/airflow:2.6.1

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pandas sqlalchemy psycopg2-binary -r requirements.txt

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

# RUN chmod -R g+rw "$HOME/.docker"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID