# data_quality_checks.py
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

# Load data for quality checks
power_data = spark.read.format("delta").load("data/delta_tables/power_data")

# Simple quality checks
def check_nulls(df, column_name):
    null_count = df.filter(df[column_name].isNull()).count()
    print(f"Null count for {column_name}: {null_count}")

def check_value_range(df, column_name, min_val, max_val):
    out_of_range = df.filter((df[column_name] < min_val) | (df[column_name] > max_val)).count()
    print(f"Out of range values for {column_name}: {out_of_range}")

check_nulls(power_data, "net_production")
check_value_range(power_data, "net_production", 0, 5000)  # Example range