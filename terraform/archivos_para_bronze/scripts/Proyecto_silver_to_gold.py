import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, count, avg, round, when, upper, trim,
    min, max, sum, countDistinct, regexp_replace
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Inicialización
args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ========================================
# LECTURA DESDE SILVER
# ========================================
input_path = "s3://datalake-silver-444655873165/Proyecto/"

output_base = "s3://datalake-gold-444655873165/Proyecto/"

renault_silver = spark.read.parquet(input_path)

# ========================================
# TRANSFORMACIÓN 1
# Matriculas por mes y año
# ========================================
matriculas_por_mes = renault_silver \
    .filter(col("año_matricula").isNotNull() & col("mes_matricula").isNotNull()) \
    .groupBy("año_matricula", "mes_matricula") \
    .agg(count("*").alias("total_matriculas")) \
    .orderBy("año_matricula", "mes_matricula")

matriculas_por_mes.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "matriculas_por_mes/")

# ========================================
# TRANSFORMACIÓN 2
# Ventas por marca y modelo
# ========================================
ventas_por_modelo = renault_silver \
    .filter(col("linea").isNotNull()) \
    .groupBy("marca", "linea", "modelo") \
    .agg(count("*").alias("total_vendidos")) \
    .orderBy(col("total_vendidos").desc())

ventas_por_modelo.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "ventas_por_modelo/")

# ========================================
# TRANSFORMACIÓN 3
# Ventas por departamento y municipio
# ========================================
ventas_por_ubicacion = renault_silver \
    .filter(col("departamento").isNotNull()) \
    .groupBy("departamento", "municipio") \
    .agg(count("*").alias("total_matriculas")) \
    .orderBy(col("total_matriculas").desc())

ventas_por_ubicacion.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "ventas_por_ubicacion/")

# ========================================
# TRANSFORMACIÓN 4
# Distribución por tipo de servicio (Particular vs otros)
# ========================================
distribucion_servicio = renault_silver \
    .filter(col("servicio").isNotNull()) \
    .groupBy("servicio") \
    .agg(count("*").alias("total")) \
    .orderBy(col("total").desc())

distribucion_servicio.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "distribucion_servicio/")

# ========================================
# TRANSFORMACIÓN 5
# Ventas por concesionario y zona
# ========================================
ventas_por_concesionario = renault_silver \
    .filter(col("concesionario").isNotNull()) \
    .groupBy("concesionario", "zona", "canal") \
    .agg(count("*").alias("total_ventas")) \
    .orderBy(col("total_ventas").desc())

ventas_por_concesionario.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "ventas_por_concesionario/")

# ========================================
# TRANSFORMACIÓN 6
# Distribución por tipo de combustible y segmento
# ========================================
combustible_segmento = renault_silver \
    .filter(col("combustible").isNotNull() & col("segmento").isNotNull()) \
    .groupBy("combustible", "segmento") \
    .agg(count("*").alias("total")) \
    .orderBy("combustible", col("total").desc())

combustible_segmento.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "combustible_por_segmento/")

# ========================================
# TRANSFORMACIÓN 7
# Peso y cilindraje promedio por clase de vehículo
# ========================================
stats_por_clase = renault_silver \
    .filter(col("clase").isNotNull()) \
    .withColumn("peso_num", regexp_replace(col("peso").cast("string"), "[^0-9.]", "").cast("double")) \
    .withColumn("cilindraje_num", col("cilindraje").cast("double")) \
    .groupBy("clase") \
    .agg(
        count("*").alias("total"),
        round(avg("peso_num"), 2).alias("peso_promedio"),
        round(avg("cilindraje_num"), 2).alias("cilindraje_promedio")
    ) \
    .orderBy(col("total").desc())

stats_por_clase.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "stats_por_clase/")

# ========================================
# TRANSFORMACIÓN 8
# Ventas por canal (Red, Flotas, etc.)
# ========================================
ventas_por_canal = renault_silver \
    .filter(col("canal").isNotNull()) \
    .groupBy("canal") \
    .agg(
        count("*").alias("total_ventas"),
        countDistinct("concesionario").alias("num_concesionarios")
    ) \
    .orderBy(col("total_ventas").desc())

ventas_por_canal.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "ventas_por_canal/")

# ========================================
# TRANSFORMACIÓN 9
# Distribución por color de vehículo
# ========================================
distribucion_color = renault_silver \
    .filter(col("color").isNotNull()) \
    .groupBy("color", "linea") \
    .agg(count("*").alias("total")) \
    .orderBy(col("total").desc())

distribucion_color.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "distribucion_color/")

# ========================================
# TRANSFORMACIÓN 10
# Vehículos con prenda vs sin prenda por departamento
# ========================================
prenda_por_departamento = renault_silver \
    .filter(col("departamento").isNotNull()) \
    .withColumn(
        "tiene_prenda",
        when(
            col("prenda").isNull() | (trim(col("prenda")) == "0") | (trim(col("prenda")) == ""),
            "SIN PRENDA"
        ).otherwise("CON PRENDA")
    ) \
    .groupBy("departamento", "tiene_prenda") \
    .agg(count("*").alias("total")) \
    .orderBy("departamento", "tiene_prenda")

prenda_por_departamento.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_base + "prenda_por_departamento/")

print("Proceso completado: Silver → Gold")