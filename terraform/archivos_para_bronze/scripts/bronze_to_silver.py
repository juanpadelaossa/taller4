import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Inicialización
args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Rutas
input_path = "s3://datalake-bronze-444655873165/taller/"
output_path = "s3://datalake-silver-444655873165/taller/"

# ========================================
# 1. LECTURA DESDE BRONZE
# ========================================

extorsion_bronze = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .csv(input_path)

# ========================================
# 2. LIMPIEZA GENERAL (TU LÓGICA)
# ========================================

extorsion_clean = extorsion_bronze


# ----------------------------------------
# 2.1 Eliminar duplicados
# ----------------------------------------

extorsion_clean = extorsion_clean.dropDuplicates()


# ----------------------------------------
# 2.2 Normalizar espacios en columnas texto
# ----------------------------------------

for c in extorsion_clean.columns:
    extorsion_clean = extorsion_clean.withColumn(
        c,
        trim(col(c))
    )


# ----------------------------------------
# 2.3 Reemplazar valores que representan nulos
# ----------------------------------------

extorsion_clean = extorsion_clean.replace(
    ["SIN DATO", "Sin dato", "NA", "N/A", "NULL", ""],
    None
)


# ----------------------------------------
# 2.4 Eliminar solo filas completamente vacías
# ----------------------------------------

extorsion_clean = extorsion_clean.dropna(how="all")


# ========================================
# 3. (OPCIONAL) ESTANDARIZACIÓN DE COLUMNAS
# ========================================

for column in extorsion_clean.columns:
    extorsion_clean = extorsion_clean.withColumnRenamed(
        column,
        column.lower().replace(" ", "_")
    )


# ========================================
# 4. ESCRITURA EN SILVER (PARQUET)
# ========================================

extorsion_clean.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path)

print("Proceso completado: Bronze → Silver")