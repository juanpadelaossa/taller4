import sys
import boto3
import pandas as pd
import io
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import great_expectations as gx

# -----------------------------------
# Inicializar Glue
# -----------------------------------
args = getResolvedOptions(sys.argv, [])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------------
# Leer datos desde Bronze (Excel)
# -----------------------------------
s3 = boto3.client("s3")
bucket_bronze = "datalake-bronze-444655873165"

def read_excel_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_excel(io.BytesIO(obj["Body"].read()))

pdf_2024 = read_excel_from_s3(bucket_bronze, "Proyecto/Base de Datos Renault 2024.xlsx")
pdf_2025 = read_excel_from_s3(bucket_bronze, "Proyecto/Base de Datos Renault 2025.xlsx")
pdf_combined = pd.concat([pdf_2024, pdf_2025], ignore_index=True)

df = spark.createDataFrame(pdf_combined)

# -----------------------------------
# Inicializar GX
# -----------------------------------
context = gx.get_context()
data_source = context.sources.add_or_update_spark(name="spark_source")
data_asset = data_source.add_dataframe_asset(name="renault_data")
batch_request = data_asset.build_batch_request(dataframe=df)

# -----------------------------------
# Expectation Suite
# -----------------------------------
suite = context.add_or_update_expectation_suite("suite_renault")
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

# -----------------------------------
# VALIDACIONES (10)
# -----------------------------------

# 1) Dataset no vacío
validator.expect_table_row_count_to_be_between(min_value=1)

# 2) Columnas esperadas presentes
validator.expect_table_columns_to_match_set(
    column_set=[
        "PLACA", "ID_VEHICULO", "AÑO_MATRICULA", "MES_MATRICULA",
        "CLASE", "MARCA", "LINEA", "MODELO",
        "SERVICIO", "DEPARTAMENTO", "MUNICIPIO",
        "COMBUSTIBLE", "CANAL", "CONCESIONARIO", "ZONA"
    ],
    exact_match=False
)

# 3) PLACA no nula
validator.expect_column_values_to_not_be_null(
    column="PLACA",
    mostly=0.99
)

# 4) MARCA siempre RENAULT
validator.expect_column_values_to_be_in_set(
    column="MARCA",
    value_set=["RENAULT"],
    mostly=0.99
)

# 5) SERVICIO en dominio válido
validator.expect_column_values_to_be_in_set(
    column="SERVICIO",
    value_set=["Particular", "Público", "Oficial", "Diplomático"],
    mostly=0.95
)

# 6) COMBUSTIBLE en dominio válido
validator.expect_column_values_to_be_in_set(
    column="COMBUSTIBLE",
    value_set=["GASOLINA", "DIESEL", "ELECTRICO", "GAS GASOL", "GASO ELEC","GNV"],
    mostly=0.95
)

# 7) AÑO_MATRICULA en rango lógico
validator.expect_column_values_to_be_between(
    column="AÑO_MATRICULA",
    min_value=2020,
    max_value=2026,
    mostly=0.99
)

# 8) MES_MATRICULA entre 1 y 12
validator.expect_column_values_to_be_between(
    column="MES_MATRICULA",
    min_value=1,
    max_value=12,
    mostly=0.99
)

# 9) DEPARTAMENTO no nulo
validator.expect_column_values_to_not_be_null(
    column="DEPARTAMENTO",
    mostly=0.95
)

# 10) CANAL en dominio válido
validator.expect_column_values_to_be_in_set(
    column="CANAL",
    value_set=["Red", "Flotas", "Digital", "Directa"],
    mostly=0.90
)

# -----------------------------------
# Ejecutar validación
# -----------------------------------
results = validator.validate()
print("===== RESULTADOS GX =====")
print(results)

# -----------------------------------
# Control de fallo para Step Function
# -----------------------------------
if not results["success"]:
    raise Exception("Data Quality FAILED: Renault Bronze")