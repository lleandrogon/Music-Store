# Databricks notebook source
source_path = '/Volumes/musicstore/logistics/resource/source'
bronze_path = '/Volumes/musicstore/logistics/resource/ETL/bronze'
silver_path = '/Volumes/musicstore/logistics/resource/ETL/silver'
gold_path = '/Volumes/musicstore/logistics/resource/ETL/gold'

# COMMAND ----------

silver_map = {
    #'silver_artist_sales': f'{silver_path}/artist_sales',
    'silver_customer_invoices': f'{silver_path}/customer_invoices',
    #'silver_track': f'{silver_path}/track'
}

for view_name, path in silver_map.items():
  spark.read.format('delta').load(path).createOrReplaceTempView(view_name)

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