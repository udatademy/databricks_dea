"""
Calcular el Importe Total y Total de Usuarios por Método de Pago
1. Unir las tablas "silver.users", "silver.trips" y "silver.payments"
2. Obtener el último "pago" del "usuario"
3. El "estado" del viaje debe ser "Completado"
4. Calcular los siguientes valores:
    - Total de Usuarios
    - Importe Total
"""

from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "tixo.gold.payment_method_summary",
    table_properties = {'quality' : 'gold'},
    comment = "Calcular el Importe Total y Total de Usuarios por Método de Pago"
)
def create_gold_payment_method_summary():

    users_df = spark.read.table("tixo.silver.users") 
    trips_df = spark.read.table("tixo.silver.trips")
    payments_df = spark.read.table("tixo.silver.payments")

    return (
        users_df
        .join(trips_df, users_df.user_id == trips_df.user_id, "inner")
        .join(payments_df, trips_df.payment_id == payments_df.payment_id, "inner")
        .filter(
            (payments_df.__END_AT.isNull()) &
            (trips_df.status == "Completed")
        )
        .groupBy(payments_df.payment_method)
        .agg(
            F.count(users_df.user_id).alias("total_users"),
            F.sum(payments_df.amount).alias("total_amount")
        )
    )