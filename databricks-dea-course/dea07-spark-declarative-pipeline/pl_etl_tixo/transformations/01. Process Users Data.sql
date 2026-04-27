/*
Procesar los datos del Usuario
1. Ingesta de datos en el data lakehouse - tixo.bronze.users
2. Realizar comprobaciones de calidad de los datos y transformación  - tixo.silver.users_clean
3. Aplicar cambios a los datos de los Usuarios (SCD Type1) - tixo.silver.users
*/

-- 1. Ingesta de datos en el data lakehouse - tixo.bronze.users
CREATE OR REFRESH STREAMING TABLE tixo.bronze.users
COMMENT 'Ingesta de los datos Users'
TBLPROPERTIES('quality' = 'bronze')
AS
SELECT *,
       _metadata.file_path AS input_file_path,
       CURRENT_TIMESTAMP AS ingest_timestamp
FROM cloud_files(
  '/Volumes/tixo/raw/operational_data/user/',
  'csv',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'header', 'true',
    'delimiter', ','
  )
);

-- 2. Realizar comprobaciones de calidad de los datos y transformación  - tixo.silver.users_clean
CREATE OR REFRESH STREAMING TABLE tixo.silver.users_clean(
  CONSTRAINT valid_user_id EXPECT(user_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_user_name EXPECT(name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT(email IS NOT NULL),
  CONSTRAINT valid_phone EXPECT(LENGTH(phone) >= 9)
)
COMMENT 'Limpiar datos Users'
TBLPROPERTIES('quality' = 'silver')
AS
SELECT user_id,
       name,
       email,
       phone,
       created_date
FROM STREAM(tixo.bronze.users);

-- 3. Aplicar cambios a los datos de los Usuarios (SCD Type1) - tixo.silver.users
CREATE OR REFRESH STREAMING TABLE tixo.silver.users
COMMENT 'Datos users - SCD Tipo 1'
TBLPROPERTIES('quality' = 'silver');

CREATE FLOW f_users
AS AUTO CDC INTO tixo.silver.users
FROM STREAM(tixo.silver.users_clean)
KEYS (user_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;







