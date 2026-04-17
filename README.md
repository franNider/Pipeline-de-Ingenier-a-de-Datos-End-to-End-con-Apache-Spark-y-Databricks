El proyecto simula un caso real de negocio (dominio funcional) e incluye todo el ciclo de vida del dato: desde la ingesta hasta la generación de insights listos para análisis.

#Principales componentes del proyecto:

##Ingesta de datos (Bronze Layer)
Generación de datos simulados en distintos formatos.
Almacenamiento en volúmenes y carga incremental usando COPY INTO.
Manejo de evolución de esquemas (schemaMerge).
Persistencia en tablas Delta Lake.

##Transformación de datos (Silver Layer)
Limpieza y estandarización de datos.
Eliminación de duplicados y tratamiento de valores nulos.
Normalización de formatos.
Conversión de tipos de datos.

##Modelado analítico (Gold Layer)
Resolución de preguntas de negocio mediante joins entre múltiples datasets y agregaciones
Generación de métricas y tablas listas para consumo analítico.

##Orquestación del pipeline
Implementación de workflows en Databricks (Jobs).
Automatización del flujo completo con ejecución programada.
Exportación del flujo en formato YAML.

#Tecnologías utilizadas:
Apache Spark (PySpark / SQL)
Databricks
Delta Lake
Unity Catalog
