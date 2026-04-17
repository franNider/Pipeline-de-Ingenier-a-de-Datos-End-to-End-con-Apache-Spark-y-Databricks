# Databricks notebook source
# MAGIC %md
# MAGIC Esta notebook toma la configuración de la orquestadora y ejecuta las transformaciones sobre cada entidad: limpia los datos, normaliza formatos y los guarda en las tablas silver.

# COMMAND ----------

# MAGIC %run ./03Orquestadora_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC Función de limpieza.
# MAGIC
# MAGIC La función de limpieza opera de forma genérica sobre cualquier entidad. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

def clean_entity(entity):
    df_clean = spark.table(entity["bronze_table"])
    
    if entity.get("drop_duplicates"):
        df_clean = df_clean.dropDuplicates(entity.get("drop_duplicates"))

    df_clean = df_clean.dropna(subset=entity.get("drop_nulls",[]))

    for c in entity.get("unknown_nulls", []):
        df_clean = df_clean.withColumn(c, F.coalesce(F.col(c), F.lit("unknown")))
    
    for c in entity.get("zero_nulls", []):
        df_clean = df_clean.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    for c in entity.get("trim_strings", []):
        df_clean = df_clean.withColumn(c, F.trim(F.col(c)))
    
    for c in entity.get("upper_strings", []):
        df_clean = df_clean.withColumn(c, F.upper(F.col(c)))
    
    for c in entity.get("cast_int", []):
        df_clean = df_clean.withColumn(c, F.col(c).cast("int"))
    
    for c in entity.get("cast_double", []):
        df_clean = df_clean.withColumn(c, F.col(c).cast("double"))

    for c in entity.get("cast_string", []):
        df_clean = df_clean.withColumn(c, F.col(c).cast("string"))
    
    for c in entity.get("cast_timestamp", []):
        df_clean = df_clean.withColumn(c, F.to_timestamp(F.col(c)))

    return df_clean


# COMMAND ----------

# MAGIC %md
# MAGIC Función de guardado en Silver.
# MAGIC
# MAGIC Se usa MERGE en lugar de overwrite para garantizar idempotencia e ingesta incremental. Si un registro ya existe en silver se actualiza, si es nuevo se inserta. Esto permite correr la notebook múltiples veces sin duplicar datos y procesar solo los cambios entre ejecuciones.

# COMMAND ----------

def save_silver(df, entity):
    silver_table= entity["silver_table"]

    df.createOrReplaceTempView("df_temp")

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING df_temp AS source
        ON target.{entity["key"]} = source.{entity["key"]}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC Ejecución de limpieza y guardado en silver.
# MAGIC
# MAGIC La ejecución itera genéricamente sobre todas las entidades definidas en la orquestadora. Agregar una entidad nueva no requiere modificar esta notebook.
# MAGIC
# MAGIC

# COMMAND ----------

for entity in ENTITIES:
    df_silver = clean_entity(entity)
    save_silver(df_silver,entity)