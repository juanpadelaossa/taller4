import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Inicialización
args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ========================================
# 1. LECTURA DESDE SILVER
# ========================================

input_path = "s3://datalake-silver-444655873165/taller/"
output_path_modalidad = "s3://datalake-gold-444655873165/taller/casos_modalidad/"
output_path_edad = "s3://datalake-gold-444655873165/taller/edad_promedio_sexo/"

extorsion_silver = spark.read.parquet(input_path)

# ========================================
# 2. TRANSFORMACIÓN 1
# Casos por modalidad
# ========================================

casos_modalidad = extorsion_silver \
    .filter(col("modalidad").isNotNull()) \
    .groupBy("modalidad") \
    .agg(
        count("*").alias("total_casos")
    ) \
    .orderBy(col("total_casos").desc())

# ========================================
# 3. TRANSFORMACIÓN 2
# Edad promedio por sexo
# ========================================

edad_promedio_sexo = extorsion_silver \
    .filter(
        (col("sexo").isNotNull()) &
        (col("edad").isNotNull()) &
        (col("edad") > 0)
    ) \
    .groupBy("sexo") \
    .agg(
        avg("edad").alias("edad_promedio")
    )

# ========================================
# 4. ESCRITURA EN GOLD
# ========================================

casos_modalidad.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path_modalidad)

edad_promedio_sexo.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path_edad)

print("Proceso completado: Silver → Gold")