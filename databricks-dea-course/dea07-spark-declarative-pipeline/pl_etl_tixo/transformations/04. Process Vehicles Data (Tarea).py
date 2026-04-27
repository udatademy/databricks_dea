"""
Procesar los datos del Vehículos
1. Ingesta de datos en el data lakehouse - tixo.bronze.vehicles
2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.vehicles_clean
3. Aplicar cambios a los datos de Vehiculos (SCD Type2) - tixo.silver.vehicles
"""
# 1. Ingesta de datos en el data lakehouse - tixo.bronze.vehicles
from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "tixo.bronze.vehicles",
    table_properties = {'quality' : 'bronze'},
    comment = "Ingesta de los datos Vehicle."
)
def create_bronze_vehicles():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/tixo/raw/operational_data/vehicle/")
             .select(
                 "*",
                 F.col("_metadata.file_path").alias("input_file_path"),
                 F.current_timestamp().alias("ingest_timestamp")
             )
    )

# 2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.vehicles_clean
@dp.table(
    name = "tixo.silver.vehicles_clean",
    table_properties = {'quality' : 'silver'},
    comment = "Limpiar datos Vehicle"
)
@dp.expect_or_fail("valid_vehicle_id", "vehicle_id IS NOT NULL")
@dp.expect_or_fail("valid_driver_id", "driver_id IS NOT NULL")
@dp.expect_or_drop("valid_plate", "LENGTH(plate) = 7")
@dp.expect("valid_year", "year >= 2020")
def create_silver_vehicles_clean():
    return (
        spark.readStream.table("tixo.bronze.vehicles")
        .select(
            "vehicle_id",
            "driver_id",
            "plate",
            "brand",
            "model",
            "year",
            F.col("created_date").cast("date").alias("created_date")
        )
    )

# 3. Aplicar cambios a los datos de Vehiculos (SCD Type2) - tixo.silver.vehicles
dp.create_streaming_table(
    name = "tixo.silver.vehicles",
    table_properties = {'quality' : 'silver'},
    comment = "Datos Vehicle - SCD Tipo 2"
)

dp.create_auto_cdc_flow(
    target = "tixo.silver.vehicles",
    source = "tixo.silver.vehicles_clean",
    keys = ["vehicle_id"],
    sequence_by = "created_date",
    stored_as_scd_type = 2
)



