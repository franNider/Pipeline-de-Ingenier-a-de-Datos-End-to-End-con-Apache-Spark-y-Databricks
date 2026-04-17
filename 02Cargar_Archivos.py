# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook corresponde a las etapas de landing e ingesta bronze. Primero invoca las funciones generadoras de datos y guarda los resultados como archivos JSON en el volume landing_marketing. Luego carga esos archivos en las tablas Delta usando COPY INTO.

# COMMAND ----------

# MAGIC %run /Workspace/TP_integrador2_Niderhaus/00_ads_data_generator

# COMMAND ----------

import json
from datetime import date

# Obtengo la fecha de ejecución
todayDate = date.today().isoformat()

# Defino las entidades y sus datos
entities = {
    "advertisers": get_advertisers(),
    "campaigns": get_campaigns(),
    "ads": get_ads(),
    "clicks": get_clicks(),
    "impressions": get_impressions(),
    "conversions": get_conversions(),
}

# Creo los archivos en el volume
for entity_name, data in entities.items():
    file_path = f"/Volumes/dev/bronze_marketing/landing_marketing/{entity_name}_{todayDate}.json"
    dbutils.fs.put(file_path, json.dumps(data), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Uso de COPY INTO para leer los archivos desde el Volume y cargarlo en las tablas deltas (ya creadas).
# MAGIC COPY INTO es idempotente (al correrse dos veces sobre un mismo archivo no duplica datos, pues recuerda que archivo procesó), al tener los archivos con la fecha facilitamos el saber los archivos ya procesados.
# MAGIC
# MAGIC Con respecto al parametro schemaMerge (parámetro que le dice a Databricks: si el archivo tiene columnas nuevas que la tabla no tiene, agregálas automáticamente en lugar de fallar), puede estar tanto en FORMAT_OPTIONS (para que entienda el schema al leer el archivo) y en COPY_OPTIONS (para que lo aplique al escribir en la tabla Delta).

# COMMAND ----------

# Lista de entidades
entities = ["advertisers", "campaigns", "ads", "clicks", "impressions", "conversions"]

# Cargo cada entidad con COPY INTO
for entity_name in entities:
    spark.sql(f"""
        COPY INTO dev.bronze_marketing.{entity_name}
        FROM '/Volumes/dev/bronze_marketing/landing_marketing/{entity_name}_*'
        FILEFORMAT = JSON
        FORMAT_OPTIONS (
            'inferSchema' = 'true',
            'mergeSchema' = 'true',
            'primitivesAsString' = 'true'
        )
        COPY_OPTIONS (
            'mergeSchema' = 'true'
        )
    """)

#opcion primitiveAsString para evitar conflicos dependiendo como llegan los datos (todos se guardan como string), en silver casteo todas las columnas de forma correcta