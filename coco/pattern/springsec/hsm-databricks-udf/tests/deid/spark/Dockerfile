# Runs the DEID Spark tests locally (tests/deid/spark/run-tests.sh): Spark 4.0 with
# Python, plus what the job and its tests need. Databricks runtimes have pyspark,
# pandas and pyarrow already; this image stands in for one.
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir \
        pandas==2.2.3 pyarrow==18.1.0 \
        cryptography==44.0.0 requests==2.32.3 certifi==2024.12.14 pyyaml==6.0.2 \
        "psycopg[binary]==3.2.3" pytest==8.3.4
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_PYTHON=python3
USER spark
