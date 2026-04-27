/*
Procesar los datos de Drivers
1. Ingesta de datos en el data lakehouse - tixo.bronze.drivers
2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.drivers_clean
3. Aplicar cambios a los datos de los Drivers (SCD Type1) - tixo.silver.drivers
*/

-- 1. Ingesta de datos en el data lakehouse - tixo.bronze.drivers
CREATE OR REFRESH STREAMING TABLE tixo.bronze.drivers
COMMENT 'Ingesta de los datos Drivers'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT *,
       _metadata.file_path AS input_file_path,
       CURRENT_TIMESTAMP AS ingest_timestamp
FROM cloud_files(
  '/Volumes/tixo/raw/operational_data/driver/',
  'csv',
  map('cloudFiles.inferColumnTypes', 'true',
      'header', 'true',
      'delimiter', ',')
);

-- 2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.drivers_clean
CREATE OR REFRESH STREAMING TABLE tixo.silver.drivers_clean(
  CONSTRAINT valid_driver_id EXPECT (driver_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_driver_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_license_number EXPECT (license_number IS NOT NULL),
  CONSTRAINT valid_phone EXPECT (LENGTH(phone) >= 9)
)
COMMENT 'Limpiar datos Drivers'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT driver_id,
       name,
       phone,
       license_number,
       status,
       created_date
FROM STREAM(tixo.bronze.drivers);

-- 3. Aplicar cambios a los datos de los Drivers (SCD Type1) - tixo.silver.drivers
CREATE OR REFRESH STREAMING TABLE tixo.silver.drivers
COMMENT 'Datos Drivers - SCD Tipo 1'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW f_drivers 
AS AUTO CDC INTO tixo.silver.drivers
FROM STREAM(tixo.silver.drivers_clean)
KEYS (driver_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;



