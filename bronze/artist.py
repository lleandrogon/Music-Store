# Databricks notebook source
display(dbutils.fs.ls('/Volumes/musicstore/logistics/resource/source/'))

# COMMAND ----------

source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

df_artist = spark.read.csv(f'{source_path}/artist.csv', header = True, inferSchema = True, sep = ',')

# COMMAND ----------

df_artist.write.mode('overwrite').format('delta').option('mergeSchema', 'true').save(f'{bronze_path}/artist')