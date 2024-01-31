FROM apache/airflow:2.5.2

# Change user to root
USER root

# Create folders
RUN mkdir /usr/local/java && mkdir /usr/local/spark

# Download dependencies
RUN apt-get update && apt-get install -y wget && apt-get install -y procps && rm -rf /var/lib/apt/lists/*
RUN wget https://download.oracle.com/java/20/latest/jdk-20_linux-x64_bin.tar.gz && tar zxvpf jdk-20_linux-x64_bin.tar.gz -C /usr/local/java && rm jdk-20_linux-x64_bin.tar.gz
RUN wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && tar zxvpf spark-3.3.2-bin-hadoop3.tgz -C /usr/local/spark && rm spark-3.3.2-bin-hadoop3.tgz
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.tar.gz && tar zxvpf mysql-connector-j-8.0.33.tar.gz -C /opt/airflow/ && rm mysql-connector-j-8.0.33.tar.gz
RUN cp /opt/airflow/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar /usr/local/spark/spark-3.3.2-bin-hadoop3/jars
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /usr/local/spark/spark-3.3.2-bin-hadoop3/jars

# Set environment variables
ENV JAVA_HOME /usr/local/java/jdk-20.0.1
ENV PATH $PATH:$JAVA_HOME/bin
ENV SPARK_HOME /usr/local/spark/spark-3.3.2-bin-hadoop3
ENV HADOOP_HOME /usr/local/spark/spark-3.3.2-bin-hadoop3
ENV PATH $PATH:$SPARK_HOME/bin

# Change user to airflow
USER airflow

# Copy requirements.txt to container
COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
