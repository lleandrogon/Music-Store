# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

display(dbutils.fs.ls(silver_path))

# COMMAND ----------

silver_map = {
    'silver_artist_sales': f'{silver_path}/artist_sales',
    'silver_customer_invoices': f'{silver_path}/customer_invoices',
    'silver_track': f'{silver_path}/track'
}

for view_name, path in silver_map.items():
    spark.read.format('delta').load(path).createOrReplaceTempView(view_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_artist_sales LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE silver_artist_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   total_sales,
# MAGIC   total_units_sold,
# MAGIC   total_revenue,
# MAGIC   CASE
# MAGIC     WHEN total_revenue > 100 THEN 'Top performer'
# MAGIC     WHEN total_revenue > 50 THEN 'Average performer'
# MAGIC     ELSE 'Emerging performer'
# MAGIC   END AS performance_tier
# MAGIC FROM silver_artist_sales
# MAGIC ORDER BY total_sales DESC;

# COMMAND ----------

df_artist_performance = spark.sql("""
SELECT
  artist_id,
  artist_name,
  total_sales,
  total_units_sold,
  total_revenue,
  CASE
    WHEN total_revenue > 100 THEN 'Top performer'
    WHEN total_revenue > 50 THEN 'Average performer'
    ELSE 'Emerging performer'
  END AS performance_tier
FROM silver_artist_sales
ORDER BY total_sales DESC;                     
                                  """)

df_artist_performance.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .save(f'{gold_path}/artist_performance')

# COMMAND ----------

df_artist_performance.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .saveAsTable('musicstore.logistics.gold_artist_performance')