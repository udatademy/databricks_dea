/*
Procesar los datos del Viajes
1. Ingesta de datos en el data lakehouse - tixo.bronze.trips
2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.trips_clean
3. Expande la matriz de elementos del objeto Trips - tixo.silver.trips
*/
-- 1. Ingesta de datos en el data lakehouse - tixo.bronze.trips
CREATE OR REFRESH STREAMING TABLE tixo.bronze.trips
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Ingesta de los datos Trips."
AS
SELECT *,
        _metadata.file_path AS input_file_path,
        CURRENT_TIMESTAMP AS ingest_timestamp
FROM cloud_files(
                  "/Volumes/tixo/raw/operational_data/trip/",
                  "json",
                  map("cloudFiles.inferColumnTypes", "true")
                );

-- 2. Realizar comprobaciones de calidad de los datos y transformación - tixo.silver.trips_clean
CREATE OR REFRESH STREAMING TABLE tixo.silver.trips_clean(
  CONSTRAINT valid_trip_id EXPECT (trip_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_driver_id EXPECT (driver_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status EXPECT (status IN ('Completed', 'In Progress'))
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Limpiar datos Trips"
AS
SELECT trip_id,
       user_id,
       driver_id,
       payment_id,
       location,
       time,
       status
FROM STREAM(tixo.bronze.trips);

-- Expande la matriz de elementos del objeto Trips - tixo.silver.trips
CREATE STREAMING TABLE tixo.silver.trips
COMMENT 'Expandir los datos Trips'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT trip_id,
       user_id,
       driver_id,
       payment_id,
       location.origin AS origin,
       location.destination AS destination,
       CAST(time.start_time AS TIMESTAMP) AS start_time,
       CAST(time.end_time AS TIMESTAMP) AS end_time,
       status
FROM (SELECT trip_id,
             user_id,
             driver_id,
             payment_id,
             explode(location) AS location,
             explode(time) AS time,
             status
      FROM STREAM(tixo.silver.trips_clean));



