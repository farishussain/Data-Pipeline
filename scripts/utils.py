import os
from delta import DeltaTable

def save_data_to_staging(spark, df, path, merge_columns):
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.alias("target").merge(
            df.alias("source"),
            " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(path)
    print(f"Data saved to staging at {path}.")