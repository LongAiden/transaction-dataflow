# FROM apache/airflow:2.8.0 AS airflow-base
# USER root
# # Install system dependencies and Java
# RUN apt-get update \
#       && apt-get install -y --no-install-recommends \
#       gnupg2 \
#       wget \
#       && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - \
#       && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
#       && apt-get update \
#       && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
#       build-essential \
#       default-libmysqlclient-dev \
#       libpq-dev \
#       temurin-11-jdk \
#       python3-dev \
#       python3-pip \
#       procps \
#       && apt-get autoremove -yqq --purge \
#       && apt-get clean \
#       && rm -rf /var/lib/apt/lists/*

# # Set JAVA_HOME and other environment variables
# ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
# ENV PATH=$PATH:$JAVA_HOME/bin
# ENV SPARK_HOME=/opt/spark
# ENV PATH=$PATH:$SPARK_HOME/bin

# # Download and install Spark (using archive URL)
# RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
#  && tar -xzf spark-3.4.1-bin-hadoop3.tgz \
#  && rm -rf /opt/spark \
#  && mv spark-3.4.1-bin-hadoop3 /opt/spark \
#  && rm spark-3.4.1-bin-hadoop3.tgz

# # Create worker-specific stage
# FROM airflow-base AS airflow-worker
# USER airflow

# # Set worker-specific environment variables
# ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
# ENV PYSPARK_PYTHON=/usr/local/bin/python

# # Install Python packages for worker
# RUN pip install --no-cache-dir \
#     pyspark==3.4.1 \
#     pydeequ==1.0.1 \
#     python-dotenv==1.0.0 \
#     nltk==3.8.1 \
#     kafka-python==2.0.2 \
#     minio==7.1.16 \
#     delta-spark==2.4.0 \
#     confluent-kafka==2.3.0 \
#     apache-airflow-providers-apache-kafka==1.3.1 \
#     apache-airflow-providers-amazon==8.18.0 \
#     apache-airflow-providers-postgres==5.10.0

# Base image with common dependencies
FROM apache/airflow:2.8.0 AS airflow-base
USER root

# Install only essential system dependencies for all services
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        libpq-dev \
        python3-dev \
        procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install common Python packages for all Airflow services
USER airflow
RUN pip install --no-cache-dir \
    python-dotenv==1.0.0 \
    apache-airflow-providers-postgres==5.10.0

# Worker-specific image that extends the base with Spark and other big data tools
FROM airflow-base AS airflow-worker
USER root

# Install Java only in the worker image
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gnupg2 \
        wget \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        temurin-11-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and install Spark only in the worker image
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar -xzf spark-3.4.1-bin-hadoop3.tgz \
    && rm -rf /opt/spark \
    && mv spark-3.4.1-bin-hadoop3 /opt/spark \
    && rm spark-3.4.1-bin-hadoop3.tgz

# Set worker-specific environment variables
USER airflow
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Install Python packages needed only for the worker
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    pydeequ==1.0.1 \
    nltk==3.8.1 \
    kafka-python==2.0.2 \
    minio==7.1.16 \
    delta-spark==2.4.0 \
    confluent-kafka==2.3.0 \
    apache-airflow-providers-apache-kafka==1.3.1 \
    apache-airflow-providers-amazon==8.18.0