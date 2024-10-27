# Use a base image with Python and Java
FROM ubuntu:22.04

# Set environment variables
ENV PYSPARK_VERSION=3.4.0
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Install dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -L -o spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark and Delta Lake
RUN pip3 install pyspark==${PYSPARK_VERSION} delta-spark

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Add Spark to PATH
ENV PATH="/opt/spark/bin:${PATH}"

CMD ["bash"]