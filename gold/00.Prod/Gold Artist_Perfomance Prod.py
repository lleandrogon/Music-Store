# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

silver_map = {
    'silver_artist_sales': f'{silver_path}/artist_sales',
    #'silver_customer_invoices': f'{silver_path}/customer_invoices',
    #'silver_track': f'{silver_path}/track'
}

for view_name, path in silver_map.items():
    spark.read.format('delta').load(path).createOrReplaceTempView(view_name)

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