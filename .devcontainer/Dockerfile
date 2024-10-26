# Use Ubuntu 22.04 as the base image instead of Debian
FROM ubuntu:22.04

# Install Java 11, curl, git, and Python with pip
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless curl git python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Upgrade pip to the latest version
RUN pip3 install --upgrade pip

# Copy requirements.txt file into the container
COPY requirements.txt .

# Install the Python packages listed in requirements.txt
RUN pip3 install -r requirements.txt

# Expose port for Jupyter Notebook
EXPOSE 8888

# Set work directory
WORKDIR /workspaces/baywa-data-pipeline