FROM apache/airflow:2.4.0
COPY entrypoint.sh /entrypoint.sh
RUN pip install pydrive
ENTRYPOINT ["/entrypoint.sh"]


