# Data Engineering Technical Challenge

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Setup and Installation](#setup-and-installation)
  - [Prerequisites](#prerequisites)
  - [Steps to Set Up](#steps-to-set-up)
- [Running the Project](#running-the-project)
- [Pipeline Details](#pipeline-details)
  - [Change Data Capture (CDC)](#change-data-capture-cdc)
  - [Data Transformations](#data-transformations)
- [Cloud Production Strategy](#cloud-production-strategy)
  - [Example Cloud Architecture](#example-cloud-architecture)
- [Assumptions and Decisions](#assumptions-and-decisions)
- [Next Steps](#next-steps)

## Project Overview
This project presents a data pipeline developed to ingest, transform, and store energy data from the Energy-Charts API. The pipeline supports BI/ML use cases, ensures data quality, and uses Change Data Capture (CDC) for consistent data updates. Data is stored in Delta tables and processed with PySpark.

## Features
- **Automated Data Ingestion**: Pipelines for public power (15-minute intervals), daily price data, and monthly installed power data.
- **CDC Logic**: Incremental updates using Delta Lake's merge functionality.
- **Data Validation**: Ensures data integrity before processing.
- **Data Transformations**: Processes data for specific BI/ML use cases.
- **Delta Lake Integration**: Utilizes Delta tables for ACID transactions and data versioning.
- **Modular Code**: Separate scripts for different pipeline stages for easy management.

## Setup and Installation

### Prerequisites
- **Docker Desktop**
- **Visual Studio Code** with the Remote - Containers extension
- **Python 3.x**

### Steps to Set Up
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/farishussain/baywa-data-pipeline.git
   cd baywa-data-pipeline
2. **Open in Visual Studio Code**: 
   Open the folder in Visual Studio Code and use the Remote - Containers extension to open the development container.
3. **Build the Container**:
   Follow the prompts in VS Code to build the container environment.

### Running the Project
**Run Data Ingestion Scripts**
Navigate to the scripts/Staging directory and run the ingestion scripts:
- python installed_power_ingest.py
- python price_ingest.py
- python public_power_ingest.py

**Apply CDC Logic**
Run the CDC scripts located in the scripts/CDC directory:
- python CDC_Installed_Power.py
- python CDC_Price.py
- python CDC_Public_Power.py

**Run Data Transformation Scripts**
Execute the transformation scripts in the scripts/Transformation directory:
- python Transform_Daily_Price_Analysis.py
- python Transform_Daily_Production_Trend.py
- python Transform_Underperformance_Prediction.py

### Pipeline Details
**Change Data Capture (CDC)**
The CDC scripts manage data consistency and updates by reading from the staging area, deduplicating data, and merging it into the CDC layer using Delta Lake:
- CDC - Installed Power (CDC_Installed_Power.py)
- CDC - Price (CDC_Price.py)
- CDC - Public Power (CDC_Public_Power.py)

**Data Transformations**
The transformation scripts process data for BI/ML use cases:
- Transform - Daily Price Analysis: Analyzes the correlation between daily prices and wind power production.
- Transform - Daily Production Trend: Summarizes daily net electricity production by type.
- Transform - Underperformance Prediction: Prepares data for predicting underperformance at 30-minute intervals.

### Cloud Production Strategy
To scale this pipeline in a cloud environment:
- Storage: Use Amazon S3 or Azure Data Lake with Delta Lake integration.
- Compute: Deploy PySpark jobs on Databricks, Amazon EMR, or a Kubernetes-based Spark cluster.
- Orchestration: Use Apache Airflow, AWS Step Functions, or Azure Data Factory.
- Monitoring: Integrate with tools like AWS CloudWatch, Datadog, or Spark's built-in monitoring UI.
**Example Cloud Architecture**
- Data Source (Energy-Charts API) → Ingestion Layer (PySpark Jobs) → Staging Layer (Delta Table in S3/Azure Blob)
- CDC Layer (Delta Table) → Final Schema (Delta Table) → BI/ML Tools (Tableau, Power BI)

### Assumptions and Decisions
- Local Execution: Data volume restricted to a manageable size.
- Data Validation: Basic data checks to ensure completeness and format correctness.
- Delta Tables: Chosen for robust data handling, ACID support, and scalable performance.
- Modular Design: Separate scripts for easy management and scalability.

### Next Steps
- Implement advanced data quality checks.
- Expand monitoring for production readiness.
- Optimize the pipeline for real-time processing if needed.
