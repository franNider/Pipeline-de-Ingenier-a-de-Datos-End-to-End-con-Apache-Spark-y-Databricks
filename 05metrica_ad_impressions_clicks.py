# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook responde la pregunta: ¿Cuál fue la performance de cada formato de ad por país durante un período específico, en términos de impresiones y clicks?

# COMMAND ----------

#En este caso a diferencia del anterior, realizo toda la tabla de una vez, los join y las operaciones sql necesarias

#Fechas a filtrar
start_date = "2025-01-01"
end_date = "2027-12-31"

#Join de las tablas que necesito para la respuesta
df_gold = spark.sql(f"""
    SELECT
        a.format,
        i.country,
        COUNT(i.id) AS total_impressions,
        COUNT(c.id) AS total_clicks,
        ROUND(total_clicks / total_impressions *100, 2) AS ctr, --porcentaje de impresiones que generaron un click
        '{start_date}' AS start_date,
        '{end_date}' AS end_date
    FROM dev.silver_marketing.ads a
    INNER JOIN dev.silver_marketing.impressions i ON a.id = i.ad_id --solo interesan los ads que tuvieron al menos una impresion. Si un ad no tuvo impresiones no aporta nada a la métrica (sin impresion no hay pais)
    LEFT JOIN dev.silver_marketing.clicks c ON i.id = c.impression_id --no toda impresion tiene un click. Con INNER JOIN solo contaria las impresiones que tuvieron click y el CTR quedaria mal calculado porque el denominador (total impresiones) sería incorrecto
    WHERE i.timestamp BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY a.format, i.country
    ORDER BY ctr DESC
""")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

#guardo en la tabla gold advertiser_impressions usando overwrite, ya que es una tabla agregada que se recalcula completa en cada ejecución
df_gold.write.format("delta").mode("overwrite").saveAsTable("dev.gold_marketing.ad_format_performance")