# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook corresponde a la etapa de configuración de infraestructura en Unity Catalog.
# MAGIC
# MAGIC Se crea el catálogo dev, los schemas bronze_marketing, silver_marketing y gold_marketing.
# MAGIC
# MAGIC Se crea el volume landing_marketing donde se depositarán los archivos crudos.
# MAGIC
# MAGIC Las tablas bronze se crean sin schema definido ya que los tipos de datos serán inferidos al momento de la ingesta. Las tablas silver y gold se crean con schema fijo, ya que son capas estandarizadas consumidas por analistas y científicos de datos.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creo el CATALOGO dev
# MAGIC CREATE CATALOG IF NOT EXISTS dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creo el SCHEMA bronze_marketing
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.bronze_marketing;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creo el VOLUME landing_marketing
# MAGIC CREATE VOLUME IF NOT EXISTS dev.bronze_marketing.landing_marketing

# COMMAND ----------

# Entidades del dominio marketing con sus tablas bronze y silver asociadas.
# El schema definido corresponde a silver; bronze se crea sin schema fijo.
TABLAS = [
    {
        "name": "advertisers",
        "bronze_table" : "dev.bronze_marketing.advertisers",
        "silver_table" : "dev.silver_marketing.advertisers",
        "schema" : """
            id INT,
            advertiser_name STRING,
            country STRING,
            industry STRING,
            website STRING
        """,
    },
    {
        "name": "campaigns",
        "bronze_table" : "dev.bronze_marketing.campaigns",
        "silver_table" : "dev.silver_marketing.campaigns",
        "schema" : """
            id INT,
            advertiser_id INT,
            campaign_name STRING,
            channel STRING,
            daily_budget_usd DOUBLE,
            objective STRING
        """,
    },
    {
        "name": "ads",
        "bronze_table" : "dev.bronze_marketing.ads",
        "silver_table" : "dev.silver_marketing.ads",
        "schema" : """
            id INT,
            campaign_id INT,
            creative_url STRING,
            format STRING
        """,
    },
    {
        "name": "impressions",
        "bronze_table" : "dev.bronze_marketing.impressions",
        "silver_table" : "dev.silver_marketing.impressions",
        "schema" : """
            id INT,
            ad_id INT,
            cost_usd DOUBLE,
            country STRING,
            `timestamp` TIMESTAMP
        """,
    },
    {
        "name": "clicks",
        "bronze_table" : "dev.bronze_marketing.clicks",
        "silver_table" : "dev.silver_marketing.clicks",
        "schema" : """
            id INT,
            impression_id INT,
            cost_usd DOUBLE,
            `timestamp` TIMESTAMP
        """,
    },
    {
        "name": "conversions",
        "bronze_table" : "dev.bronze_marketing.conversions",
        "silver_table" : "dev.silver_marketing.conversions",
        "schema" : """
            click_id INT,
            value_usd DOUBLE,
            id INT,
            `timestamp` TIMESTAMP
        """,
    }
]

# COMMAND ----------

# Metricas del dominio marketing con su tabla gold asociada.
# El schema de cada métrica es fijo y no debe modificarse.
TABLAS_GOLD = [
    {
        "gold_table": "dev.gold_marketing.ad_format_performance",
        "schema": """
            format STRING,
            country STRING,
            total_impressions LONG,
            total_clicks LONG,
            ctr DOUBLE,
            start_date STRING,
            end_date STRING
        """
    },
    {
        "gold_table": "dev.gold_marketing.advertiser_impressions",
        "schema": """
            advertiser_id INT,
            advertiser_name STRING,
            advertiser_country STRING,
            total_cost DOUBLE,
            total_impressions LONG
        """
    }
]

# COMMAND ----------

#Creo las 6 tablas bronze
for tabla in TABLAS:
  spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {tabla['bronze_table']} USING DELTA
  """)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Creo el SCHEMA silver_marketing
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.silver_marketing;

# COMMAND ----------

for tabla in TABLAS:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabla['silver_table']} (
            {tabla['schema']}
        ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creo el SCHEMA gold_marketing
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.gold_marketing;

# COMMAND ----------

# Creo las tablas gold con schema fijo
for tabla in TABLAS_GOLD:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabla['gold_table']} (
            {tabla['schema']}
        ) USING DELTA
    """)