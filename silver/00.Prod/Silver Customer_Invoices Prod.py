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

df_customer_invoices = spark.sql("""
WITH customer_invoice AS (
  SELECT
    ivc.customer_id,
    ivc.invoice_id,
    CAST(ivc.invoice_date AS DATE) AS invoice_date,
    SUM(ROUND(ivc.total, 2)) AS total
  FROM bronze_invoice AS ivc
  GROUP BY ivc.customer_id, ivc.invoice_id, ivc.invoice_date
)

SELECT
  ctm.customer_id,
  CONCAT(ctm.first_name, ' ', ctm.last_name) AS name,
  ctm.email,
  COALESCE(ctm.phone, 'Unknown') AS customer_phone,
  COALESCE(ctm.company, 'Unknown') AS company,
  ctm.address,
  ctm.city,
  COALESCE(ctm.state, 'Unknown') AS state,
  ctm.country,
  COALESCE(ctm.postal_code, 'Unknown') AS customer_postal_code,
  ivc.invoice_id,
  DATE_FORMAT(ivc.invoice_date, 'dd/MM/yyyy') AS invoice_date,
  ivc.total,
  ctm.support_rep_id AS support_id,
  CONCAT(emp.first_name, ' ', emp.last_name) AS support_name, 
  emp.email AS support_email,
  emp.phone AS support_phone,
  emp.title AS support_title,
  emp.postal_code AS support_postal_code
FROM bronze_customer AS ctm
RIGHT JOIN customer_invoice AS ivc ON ctm.customer_id = ivc.customer_id
LEFT JOIN bronze_employee AS emp ON ctm.support_rep_id = emp.employee_id
ORDER BY customer_id;                                
                                """)

df_customer_invoices.write.mode('overwrite')\
  .format('delta')\
  .option('mergeSchema', 'true')\
  .save(f'{silver_path}/customer_invoices')