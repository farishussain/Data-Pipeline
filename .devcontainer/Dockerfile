# Use a base image with Python and Java
FROM ubuntu:22.04

# Set environment variables
ENV PYSPARK_VERSION=3.4.0
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:/opt/spark/bin:${PATH}"

# Install dependencies including Git
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl python3 python3-pip git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ensure python command is available as an alias for python3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark and Delta Lake
RUN pip3 install pyspark==${PYSPARK_VERSION} delta-spark

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Default command
CMD ["bash"]