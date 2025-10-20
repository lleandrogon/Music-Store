# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

bronze_map = {
    'bronze_album': f'{bronze_path}/album',
    'bronze_artist': f'{bronze_path}/artist',
    'bronze_customer': f'{bronze_path}/customer',
    'bronze_employee': f'{bronze_path}/employee',
    'bronze_genre': f'{bronze_path}/genre',
    'bronze_invoice': f'{bronze_path}/invoice',
    'bronze_invoice_line': f'{bronze_path}/invoice_line',
    'bronze_media_type': f'{bronze_path}/media_type',
    'bronze_playlist': f'{bronze_path}/playlist',
    'bronze_playlist_track': f'{bronze_path}/playlist_track',
    'bronze_track': f'{bronze_path}/track'
}

for view_name, path in bronze_map.items():
    spark.read.format('delta').load(path).createOrReplaceTempView(view_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_artist LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_artist;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_genre;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_invoice LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_invoice;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_invoice_line LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_invoice_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_track LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_track;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_album;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   art.artist_id,
# MAGIC   art.name AS artist_name,
# MAGIC   COUNT(ivl.invoice_line_id) AS total_sales,
# MAGIC   SUM(ivl.quantity) AS total_units_sold,
# MAGIC   ROUND(SUM(ivl.unit_price * ivl.quantity), 2) AS total_revenue
# MAGIC FROM bronze_artist AS art
# MAGIC JOIN bronze_album AS alb ON art.artist_id = alb.artist_id
# MAGIC JOIN bronze_track AS tck ON alb.album_id = TRY_CAST(tck.album_id AS INT)
# MAGIC JOIN bronze_invoice_line AS ivl ON tck.track_id = ivl.track_id
# MAGIC GROUP BY art.artist_id, artist_name
# MAGIC ORDER BY artist_id;

# COMMAND ----------

df_artist_sales = spark.sql("""
SELECT
  art.artist_id,
  art.name AS artist_name,
  COUNT(ivl.invoice_line_id) AS total_sales,
  SUM(ivl.quantity) AS total_units_sold,
  ROUND(SUM(ivl.unit_price * ivl.quantity), 2) AS total_revenue
FROM bronze_artist AS art
JOIN bronze_album AS alb ON art.artist_id = alb.artist_id
JOIN bronze_track AS tck ON alb.album_id = TRY_CAST(tck.album_id AS INT)
JOIN bronze_invoice_line AS ivl ON tck.track_id = ivl.track_id
GROUP BY art.artist_id, artist_name
ORDER BY artist_id;
                            """)

df_artist_sales.write.mode('overwrite')\
    .format('delta')\
    .option('inferSchema', 'true')\
    .save(f'{silver_path}/artist_sales')

# COMMAND ----------

df_artist_sales.write.mode('overwrite')\
    .format('delta')\
    .option('inferSchema', 'true')\
    .saveAsTable('musicstore.logistics.artist_sales')