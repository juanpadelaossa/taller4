import sys
import boto3
import pandas as pd
import io
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Inicialización
args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuración
bucket_bronze = "datalake-bronze-444655873165"
output_path = "s3://datalake-silver-444655873165/Proyecto/"

s3 = boto3.client("s3")

# ========================================
# 1. LECTURA DESDE BRONZE (DOS EXCEL)
# ========================================
def read_excel_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_excel(io.BytesIO(obj["Body"].read()))

pdf_2024 = read_excel_from_s3(bucket_bronze, "Proyecto/Base de Datos Renault 2024.xlsx")
pdf_2025 = read_excel_from_s3(bucket_bronze, "Proyecto/Base de Datos Renault 2025.xlsx")

# ========================================
# 2. UNIÓN Y CONVERSIÓN A SPARK
# ========================================
pdf_combined = pd.concat([pdf_2024, pdf_2025], ignore_index=True)
renault_bronze = spark.createDataFrame(pdf_combined)

# ========================================
# 3. LIMPIEZA GENERAL
# ========================================
renault_clean = renault_bronze

# 3.1 Eliminar duplicados
renault_clean = renault_clean.dropDuplicates()

# 3.2 Normalizar espacios en columnas texto
for c in renault_clean.columns:
    renault_clean = renault_clean.withColumn(c, trim(col(c)))

# 3.3 Reemplazar valores que representan nulos
renault_clean = renault_clean.replace(
    ["SIN DATO", "Sin dato", "NA", "N/A", "NULL", ""],
    None
)

# 3.4 Eliminar filas completamente vacías
renault_clean = renault_clean.dropna(how="all")

# ========================================
# 4. ESTANDARIZACIÓN DE COLUMNAS
# ========================================
for column in renault_clean.columns:
    renault_clean = renault_clean.withColumnRenamed(
        column,
        column.lower().replace(" ", "_")
    )

# ========================================
# 5. ESCRITURA EN SILVER (PARQUET)
# ========================================
renault_clean.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path)

print("Proceso completado: Bronze → Silver")