# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

display(dbutils.fs.ls(f'{bronze_path}'))

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
# MAGIC SELECT * FROM bronze_album LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_album;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_track LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_track;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_genre LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_media_type LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_media_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   tck.track_id,
# MAGIC   tck.name AS track,
# MAGIC   COALESCE(alb.title, 'Unknown album') AS album,
# MAGIC   COALESCE(art.name, 'Unknown artist') AS artist,
# MAGIC   gen.name AS genre,
# MAGIC   COALESCE(tck.composer, 'Unknown composer') AS composer,
# MAGIC   CONCAT(
# MAGIC     FLOOR(TRY_CAST(tck.milliseconds AS INT) / 60000),
# MAGIC     ':',
# MAGIC     LPAD(FLOOR(TRY_CAST(tck.milliseconds AS INT) % 60000 / 1000), 2, '0')
# MAGIC   ) AS duration,
# MAGIC   mt.name AS media,
# MAGIC   tck.unit_price
# MAGIC FROM bronze_track AS tck
# MAGIC LEFT JOIN bronze_album AS alb ON TRY_CAST(tck.album_id AS INT) = alb.album_id
# MAGIC LEFT JOIN bronze_artist AS art ON alb.artist_id = art.artist_id
# MAGIC LEFT JOIN bronze_genre AS gen ON tck.genre_id = gen.genre_id
# MAGIC LEFT JOIN bronze_media_type AS mt ON TRY_CAST(tck.media_type_id AS INT) = mt.media_type_id;

# COMMAND ----------

df_track = spark.sql('''
SELECT
  tck.track_id,
  tck.name AS track,
  COALESCE(alb.title, 'Unknown album') AS album,
  COALESCE(art.name, 'Unknown artist') AS artist,
  gen.name AS genre,
  COALESCE(tck.composer, 'Unknown composer') AS composer,
  CONCAT(
    FLOOR(TRY_CAST(tck.milliseconds AS INT) / 60000),
    ':',
    LPAD(FLOOR(TRY_CAST(tck.milliseconds AS INT) % 60000 / 1000), 2, '0')
  ) AS duration,
  mt.name AS media,
  tck.unit_price
FROM bronze_track AS tck
LEFT JOIN bronze_album AS alb ON TRY_CAST(tck.album_id AS INT) = alb.album_id
LEFT JOIN bronze_artist AS art ON alb.artist_id = art.artist_id
LEFT JOIN bronze_genre AS gen ON tck.genre_id = gen.genre_id
LEFT JOIN bronze_media_type AS mt ON TRY_CAST(tck.media_type_id AS INT) = mt.media_type_id;
                     ''')

df_track.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .save(f'{silver_path}/track')

# COMMAND ----------

df_track.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .saveAsTable('musicstore.logistics.silver_track')