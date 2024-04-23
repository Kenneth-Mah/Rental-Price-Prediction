FROM apache/airflow:2.8.3

USER root
RUN apt-get -y update
RUN apt-get -y install git

USER airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt