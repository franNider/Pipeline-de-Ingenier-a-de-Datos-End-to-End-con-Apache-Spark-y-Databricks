# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook define qué entidades existen, qué transformaciones aplicar a cada una y crea las tablas silver vacías donde se van a guardar los datos limpios. Es el único lugar que hay que tocar si se quiere agregar una entidad nueva.

# COMMAND ----------

# MAGIC %md
# MAGIC Las tablas elegidas son: advertisers, campaigns, ads, impressions.
# MAGIC Las imprimo para analizar el contendio, el formato de los datos y tipo de columnas entre otros.

# COMMAND ----------

# MAGIC %md
# MAGIC Orquestación de entidades, elijo las tablas a tratar y que limpieza realizar.

# COMMAND ----------

#Selecciono todas las entidades, y elijo que aplicar a cada una como limpieza

#drop_duplicates: elimina filas con valores duplicados (especialmente usado en las claves primarias)
#drop_nulls: elimina filas con valores nulos (especialmente usado en las claves primarias)
#unknown_nulls: reemplaza valores nulos por "unknown"
#zero_nulls: reemplaza valores nulos por 0
#trim_strings: elimina espacios en blanco al inicio y al final de los strings
#upper_strings: convierte los strings a mayúsculas
#cast_int: convierte las columnas a enteros
#cast_double: convierte las columnas a double
#cast_string: convierte las columnas a string
#cast_timestamp: convierte las columnas a timestamps

ENTITIES = [
    {
        "name": "advertisers",
        "bronze_table" : "dev.bronze_marketing.advertisers",
        "silver_table" : "dev.silver_marketing.advertisers",
        "key" : "id",
        "drop_duplicates" : ["id"],
        "drop_nulls" : ["id"],
        "unknown_nulls" : ["advertiser_name", "country", "industry"],
        "trim_strings" : ["advertiser_name", "country", "industry", "website"],
        "upper_strings" : ["country"],
        "cast_int" : ["id"],
        "cast_string" : ["advertiser_name", "country", "industry", "website"],
    },
    {
        "name": "campaigns",
        "bronze_table" : "dev.bronze_marketing.campaigns",
        "silver_table" : "dev.silver_marketing.campaigns",
        "key" : "id",
        "drop_duplicates" : ["id"],
        "drop_nulls" : ["id", "advertiser_id"],
        "unknown_nulls" : ["campaign_name", "channel", "objective"],
        "zero_nulls" : ["daily_budget_usd"],
        "trim_strings" : ["campaign_name", "channel", "objective"],
        "cast_int" : ["id", "advertiser_id"],
        "cast_double" : ["daily_budget_usd"],
        "cast_string" : ["campaign_name", "channel", "objective"],
    },
    {
        "name": "ads",
        "bronze_table" : "dev.bronze_marketing.ads",
        "silver_table" : "dev.silver_marketing.ads",
        "key" : "id",
        "drop_duplicates" : ["id"],
        "drop_nulls" : ["id", "campaign_id"],
        "unknown_nulls" : ["creative_url", "format"],
        "trim_strings" : ["creative_url", "format"],
        "cast_int" : ["id", "campaign_id"],
        "cast_string" : ["creative_url", "format"],
    },
    {
        "name": "impressions",
        "bronze_table" : "dev.bronze_marketing.impressions",
        "silver_table" : "dev.silver_marketing.impressions",
        "key" : "id",
        "drop_duplicates" : ["id"],
        "drop_nulls" : ["id", "ad_id"],
        "unknown_nulls" : ["country"],
        "zero_nulls" : ["cost_usd"],
        "trim_strings" : ["country"],
        "upper_strings" : ["country"],
        "cast_int" : ["id", "ad_id"],
        "cast_double" : ["cost_usd"],
        "cast_string" : ["country"],
        "cast_timestamp" : ["timestamp"],
    },
    {
        "name": "clicks",
        "bronze_table" : "dev.bronze_marketing.clicks",
        "silver_table" : "dev.silver_marketing.clicks",
        "key" : "id",
        "drop_duplicates": ["id"],
        "cast_int" : ["id", "impression_id"],
        "cast_double" : ["cost_usd"],
        "cast_timestamp" : ["timestamp"],
    },
    {
        "name": "conversions",
        "bronze_table" : "dev.bronze_marketing.conversions",
        "silver_table" : "dev.silver_marketing.conversions",
        "key" : "id",
        "drop_duplicates": ["id"],
        "cast_int" : ["id", "click_id"],
        "cast_double" : ["value_usd"],
        "cast_timestamp" : ["timestamp"],
    },
]