"""
Procesar los datos del Pagos
1. Ingesta de datos en el data lakehouse - tixo.bronze.payments
2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.payments_clean
3. Aplicar cambios a los datos de Pagos (SCD Type2) - tixo.silver.payments
"""
# 1. Ingesta de datos en el data lakehouse - tixo.bronze.payments
from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "tixo.bronze.payments",
    table_properties = {'quality' : 'bronze'},
    comment = "Ingesta de los datos Payments."
)
def create_bronze_payments():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/tixo/raw/operational_data/payment/")
             .select(
                 "*",
                 F.col("_metadata.file_path").alias("input_file_path"),
                 F.current_timestamp().alias("ingest_timestamp")
             )
    )

# 2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.payments_clean
@dp.table(
    name = "tixo.silver.payments_clean",
    table_properties = {'quality' : 'silver'},
    comment = "Limpiar datos Payments"
)
@dp.expect_or_fail("valid_payment_id", "payment_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount IS NOT NULL")
@dp.expect("valid_payment_method", "payment_method IN ('Card', 'Cash', 'Wallet')")
def create_silver_payments_clean():
    return(
        spark.readStream.table("tixo.bronze.payments")
        .select(
            "payment_id",
            "amount",
            "payment_method",
            F.col("payment_date").cast("date").alias("payment_date"),
            "payment_status",
            F.col("created_date").cast("date").alias("created_date")
        )
    )

# 3. Aplicar cambios a los datos de Pagos (SCD Type2) - tixo.silver.payments
dp.create_streaming_table(
    name = "tixo.silver.payments",
    table_properties = {'quality' : 'silver'},
    comment = "Datos Payments - SCD Tipo 2"
)

dp.create_auto_cdc_flow(
    target = "tixo.silver.payments",
    source = "tixo.silver.payments_clean",
    keys = ["payment_id"],
    sequence_by = "created_date",
    stored_as_scd_type = 2
)















