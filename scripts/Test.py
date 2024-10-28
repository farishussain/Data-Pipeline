import delta
print("Delta version:", delta.__version__)

Spark version: 3.4.0
.config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
root@cb83b03ecd63:/workspaces/baywa-data-pipeline# pip show delta-spark
Name: delta-spark
Version: 2.4.0
Summary: Python APIs for using Delta Lake with Apache Spark
Home-page: https://github.com/delta-io/delta/
Author: The Delta Lake Project Authors
Author-email: delta-users@googlegroups.com
License: Apache-2.0
Location: /usr/local/lib/python3.10/dist-packages
Requires: importlib-metadata, pyspark
Required-by: 