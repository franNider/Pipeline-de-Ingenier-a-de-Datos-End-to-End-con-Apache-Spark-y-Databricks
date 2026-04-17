# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook responde la pregunta: ¿Qué advertisers tuvieron más impresiones y cuál fue su costo total?

# COMMAND ----------

#Join de las tablas que necesito para la respuesta
df_complete = spark.sql("""
    SELECT
        a.id AS advertiser_id,
        a.advertiser_name,
        a.country AS advertiser_country,
        i.id AS impression_id,
        i.cost_usd
    FROM dev.silver_marketing.advertisers a
    INNER JOIN dev.silver_marketing.campaigns c ON a.id = c.advertiser_id
    INNER JOIN dev.silver_marketing.ads ad ON c.id = ad.campaign_id
    INNER JOIN dev.silver_marketing.impressions i ON ad.id = i.ad_id
""")

#Se usa INNER JOIN en toda la cadena porque el objetivo es analizar unicamente los advertisers que tuvieron impresiones reales. Un advertiser sin campañas, o una campaña sin ads, o un ad sin impresiones no aporta información útil a esta métrica.

# COMMAND ----------

display(df_complete)

# COMMAND ----------

#creo una vista temporal para trabajar con SQL
df_complete.createOrReplaceTempView("tabla")

# COMMAND ----------

df_gold = spark.sql("""
    SELECT advertiser_id, advertiser_name, advertiser_country, ROUND(SUM(cost_usd),2) AS total_cost, COUNT(impression_id) AS total_impressions
    FROM tabla
    GROUP BY advertiser_id, advertiser_name, advertiser_country
    ORDER BY total_impressions DESC
""")


# COMMAND ----------

display(df_gold)

# COMMAND ----------

#guardo en la tabla gold advertiser_impressions usando overwrite, ya que es una tabla agregada que se recalcula completa en cada ejecución
df_gold.write.format("delta").mode("overwrite").saveAsTable("dev.gold_marketing.advertiser_impressions")