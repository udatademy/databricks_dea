/*
Calcular los Ingresos por Conductor
1. Unir las tablas "silver.drivers", "silver.trips" y "silver.payments"
2. Obtener el último "pago" del "usuario"
3. El "estado" del viaje debe ser "Completado"
4. Calcular el siguiente valor:
    - Ingresos Totales
*/

CREATE OR REFRESH MATERIALIZED VIEW tixo.gold.driver_revenue_summary
COMMENT "Calculas los Ingresos por Conductor"
TBLPROPERTIES ("quality" = 'gold')
AS
SELECT d.driver_id,
       d.name,
       SUM(p.amount) AS total_revenue
FROM tixo.silver.drivers d
JOIN tixo.silver.trips t ON d.driver_id = t.driver_id
JOIN tixo.silver.payments p ON t.payment_id = p.payment_id
WHERE p.__END_AT IS NULL AND t.status = 'Completed'
GROUP BY ALL;