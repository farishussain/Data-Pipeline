# .devcontainer/Dockerfile
FROM python:3.9-slim

# Install Java (required by Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variable for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install PySpark and Delta Lake dependencies
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

# Expose port for Jupyter Notebook
EXPOSE 8888

# Set work directory
WORKDIR /workspaces/baywa-data-pipeline