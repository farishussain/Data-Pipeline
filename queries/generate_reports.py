# generate_reports.py
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Reports") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

def run_daily_trend_report():
    query = """
        SELECT date, production_type, SUM(net_electricity) as total_net_electricity
        FROM final.public_power_trend
        GROUP BY date, production_type
        ORDER BY date
    """
    result = spark.sql(query)
    result.show()

def run_underperformance_prediction():
    query = """
        SELECT timestamp, production_type, value, expected_value,
               CASE WHEN value < expected_value THEN 'Underperformance' ELSE 'Normal' END as underperformance
        FROM final.public_power_30min
        ORDER BY timestamp
    """
    result = spark.sql(query)
    result.show()

def run_price_vs_wind_power_analysis():
    query = """
        SELECT date, average_price, offshore_net_power, onshore_net_power
        FROM final.price_vs_wind_power
        ORDER BY date
    """
    result = spark.sql(query)
    result.show()

# Run reports
run_daily_trend_report()
run_underperformance_prediction()
run_price_vs_wind_power_analysis()