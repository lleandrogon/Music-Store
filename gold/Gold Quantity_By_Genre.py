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
# MAGIC SELECT * FROM silver_track WHERE genre = 'Jazz' ORDER BY unit_price DESC LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE silver_track;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   genre,
# MAGIC   COUNT(track_id) AS quantity_by_genre,
# MAGIC   ROUND(AVG(TRY_CAST(unit_price AS DOUBLE)), 2) AS average_price
# MAGIC FROM silver_track
# MAGIC WHERE genre IS NOT NULL AND TRY_CAST(unit_price AS DOUBLE) < 1000000
# MAGIC GROUP BY genre;

# COMMAND ----------

df_quantity_genre = spark.sql("""
SELECT
  genre,
  COUNT(track_id) AS quantity_by_genre,
  ROUND(AVG(TRY_CAST(unit_price AS DOUBLE)), 2) AS average_price
FROM silver_track
WHERE genre IS NOT NULL AND TRY_CAST(unit_price AS DOUBLE) < 1000000
GROUP BY genre;
                              """)

df_quantity_genre.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .save(f'{gold_path}/quantity_by_genre')

# COMMAND ----------

df_quantity_genre.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .saveAsTable('musicstore.logistics.gold_quantity_by_genre')