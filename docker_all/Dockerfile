FROM apache/airflow:2.8.0 AS spark-base
USER root
# Install Java
RUN apt-get update &&
    apt-get install -y --no-install-recommends \
        gnupg2 \
        wget &&
    wget -O /etc/apt/trusted.gpg.d/adoptium.asc https://packages.adoptium.net/artifactory/api/gpg/key/public &&
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list &&
    apt-get update &&
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        temurin-11-jdk &&
    apt-get autoremove -yqq --purge &&
    apt-get clean &&
    rm -rf /var/lib/apt/lists/*
# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
# Download and install Spark
RUN mkdir -p /opt/spark &&
    wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz &&
    tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt/spark --strip-components=1 &&
    rm spark-3.4.1-bin-hadoop3.tgz
# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=/usr/local/bin/python

FROM apache/airflow:2.8.0 AS airflow-base
# Copy Spark installation from spark-base
COPY --from=spark-base /opt/spark /opt/spark
COPY --from=spark-base $JAVA_HOME /usr/lib/jvm/temurin-11-jdk-amd64
USER root

# Remove existing symlink or directory if it exists, then create a new one
RUN [ -e /usr/lib/jvm/java-11-openjdk-amd64 ] && rm -rf /usr/lib/jvm/java-11-openjdk-amd64 || true &&
    ln -s /usr/lib/jvm/temurin-11-jdk-amd64 /usr/lib/jvm/java-11-openjdk-amd64

# Set Java and Spark environment variables again
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Switch to airflow user for pip installations
USER airflow
# Install Python packages
RUN pip install --no-cache-dir --upgrade pip &&
    pip install --no-cache-dir \
        python-dotenv==1.0.0 \
        apache-airflow-providers-postgres==5.10.0 \
        pyspark==3.4.1 \
        pydeequ==1.0.1 \
        nltk==3.8.1 \
        kafka-python==2.0.2 \
        minio==7.1.16 \
        delta-spark==2.4.0 \
        confluent-kafka==2.3.0 \
        apache-airflow-providers-apache-kafka==1.3.1 \
        apache-airflow-providers-amazon==8.18.0 \
        apache-flink==1.17.1 \
        sqlalchemy \
        psycopg2-binary \
        pandas \
        numpy \
        feast==0.36.0\
        feast-spark==0.2.37

FROM airflow-base AS airflow-worker
# This stage now inherits everything from airflow-base
