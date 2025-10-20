# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

df_employee = spark.read.csv(f'{source_path}/employee.csv', header = True, inferSchema = True, sep = ',')

# COMMAND ----------

df_employee.write.mode('overwrite').format('delta').option('mergeSchema', 'true').save(f'{bronze_path}/employee')