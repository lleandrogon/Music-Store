# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

display(dbutils.fs.ls(f'{silver_path}'))

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
# MAGIC SELECT * FROM silver_customer_invoices WHERE country IN ('Brazil', 'Argentina', 'Uruguay', 'Colombia', 'Chile', 'Paraguay', 'Peru', 'Ecuador', 'Bolivia', 'Venezuela', 'Suriname', 'Guyana');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE silver_customer_invoices;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN country IN ('Brazil', 'Argentina', 'Uruguay', 'Colombia', 'Chile', 'Paraguay', 'Peru', 'Ecuador', 'Bolivia', 'Venezuela', 'Suriname', 'Guyana') THEN 'South America'
# MAGIC     WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'North America'
# MAGIC     WHEN country IN ('Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama') THEN 'Central America'
# MAGIC   END AS continent,
# MAGIC   COUNT(invoice_id) AS total_invoices,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   ROUND(SUM(total), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total), 2) AS avg_invoice_value
# MAGIC FROM silver_customer_invoices
# MAGIC WHERE country IN ('Brazil', 'Argentina', 'Uruguay', 'Colombia', 'Chile', 'Paraguay', 'Peru', 'Ecuador', 'Bolivia', 'Venezuela', 'Suriname', 'Guyana', 'USA', 'Canada', 'Mexico', 'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama')
# MAGIC GROUP BY continent;

# COMMAND ----------

df_america_invoices = spark.sql("""
SELECT
  CASE
    WHEN country IN ('Brazil', 'Argentina', 'Uruguay', 'Colombia', 'Chile', 'Paraguay', 'Peru', 'Ecuador', 'Bolivia', 'Venezuela', 'Suriname', 'Guyana') THEN 'South America'
    WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'North America'
    WHEN country IN ('Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama') THEN 'Central America'
  END AS continent,
  COUNT(invoice_id) AS total_invoices,
  COUNT(DISTINCT customer_id) AS unique_customers,
  ROUND(SUM(total), 2) AS total_revenue,
  ROUND(AVG(total), 2) AS avg_invoice_value
FROM silver_customer_invoices
WHERE country IN ('Brazil', 'Argentina', 'Uruguay', 'Colombia', 'Chile', 'Paraguay', 'Peru', 'Ecuador', 'Bolivia', 'Venezuela', 'Suriname', 'Guyana', 'USA', 'Canada', 'Mexico', 'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama')
GROUP BY continent;                            
                                """)

df_america_invoices.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .save(f'{gold_path}/america_invoices')

# COMMAND ----------

df_america_invoices.write.mode('overwrite')\
    .format('delta')\
    .option('mergeSchema', 'true')\
    .saveAsTable('musicstore.logistics.gold_america_invoices')