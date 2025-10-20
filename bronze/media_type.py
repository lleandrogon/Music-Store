# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

df_media_type = spark.read.csv(f'{source_path}/media_type.csv', header = True, inferSchema = True, sep = ',')

# COMMAND ----------

df_media_type.write.mode('overwrite').format('delta').option('mergeSchema', 'true').save(f'{bronze_path}/media_type')