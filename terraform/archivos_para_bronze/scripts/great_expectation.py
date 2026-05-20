import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import great_expectations as gx

# -----------------------------------
# 🔧 Inicializar Glue
# -----------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------------
# Leer datos (S3) – extorsion.csv
# -----------------------------------
df = spark.read.option("header", True).option("delimiter", ";").csv(
    "s3://datalake-bronze-444655873165/taller/extorsion.csv"
)

# -----------------------------------
# Inicializar GX
# -----------------------------------
context = gx.get_context()

data_source = context.sources.add_or_update_spark(name="spark_source")
data_asset = data_source.add_dataframe_asset(name="extorsion_data")

batch_request = data_asset.build_batch_request(dataframe=df)

# -----------------------------------
# Expectation Suite
# -----------------------------------
suite = context.add_or_update_expectation_suite("suite_extorsion")

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

# -----------------------------------
# ✅ VALIDACIONES (15)
# -----------------------------------

# 1) Dataset no vacío
validator.expect_table_row_count_to_be_between(min_value=1)

validator.expect_table_columns_to_match_set(
    column_set=[
        "fecha_hecho",
        "cantidad",
        "sexo",
        "edad",
        "conducta",
        "fecha_ingestion",
        "latitud",
        "longitud"
    ],
    exact_match=False
)

# 3) Conducta = ExtorsiÃ³n (literal del archivo)
validator.expect_column_values_to_match_regex(
    column="conducta",
    regex="^Extorsi",
)

# 4) fecha_hecho no nula
validator.expect_column_values_to_not_be_null(
    column="fecha_hecho"
)

# 5) fecha_ingestion no nula
validator.expect_column_values_to_not_be_null(
    column="fecha_ingestion"
)

# 6) cantidad siempre igual a 1
validator.expect_column_values_to_be_between(
    column="cantidad",
    min_value=1,
    max_value=1
)

# 7) Edad lógica para autores del delito (mostly)
validator.expect_column_values_to_be_between(
    column="edad",
    min_value=12,
    max_value=90,
    mostly=0.80
)

# 8) Sexo en dominio válido con alto porcentaje informado
validator.expect_column_values_to_be_in_set(
    column="sexo",
    value_set=["Hombre", "Mujer", "Sin dato"],
    mostly=0.95
)

# 9) Latitud válida (con tolerancia a nulos)
validator.expect_column_values_to_be_between(
    column="latitud",
    min_value=-90,
    max_value=90,
    mostly=0.95
)

# 10) Longitud válida (con tolerancia a nulos)
validator.expect_column_values_to_be_between(
    column="longitud",
    min_value=-180,
    max_value=180,
    mostly=0.95
)

# 11) fecha_hecho con formato fecha
validator.expect_column_values_to_match_regex(
    column="fecha_hecho",
    regex=r"^\d{4}-\d{2}-\d{2}",
    mostly=0.95
)

# 12) fecha_ingestion con formato fecha
validator.expect_column_values_to_match_regex(
    column="fecha_ingestion",
    regex=r"^\d{4}-\d{2}-\d{2}",
    mostly=0.95
)

# 13) conducta no nula
validator.expect_column_values_to_not_be_null(
    column="conducta",
    mostly=0.99
)

# 14) edad informada en la mayoría de los casos
validator.expect_column_values_to_not_be_null(
    column="edad",
    mostly=0.85
)

# 15) cantidad no nula
validator.expect_column_values_to_not_be_null(
    column="cantidad",
    mostly=0.99
)

# -----------------------------------
#  Ejecutar validación
# -----------------------------------
results = validator.validate()

print("===== RESULTADOS GX =====")
print(results)

# -----------------------------------
# Control de fallo (MUY IMPORTANTE)
# -----------------------------------
if not results["success"]:
    raise Exception("Data Quality FAILED")