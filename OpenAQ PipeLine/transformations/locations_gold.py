import dlt
from pyspark.sql import functions as F

@dlt.table(
    name= "GOLD_DAILY_SUMMARY",
    comment="Promedios y picos diarios de los contaminantes agrupado por ciudad!"
)
def gold_daily_summary():
    return (
        dlt.read("SILVER_GLOBAL_OPENAQ")
        .withColumn("date", F.to_date("utc_date"))
        .groupBy("city", "parameter", "date")
        .agg(
            F.round(F.avg("value"),2).alias("avg_value"),
            F.max("Value").alias("max_value"),
            F.min("Value").alias("min_value")
        )
    )

@dlt.table(
    name="GOLD_ML_FEATURES",
    comment="Tabla pivotada por hora, ideal para entrenamiento de modelos."
)
def gold_ml_features():
    return (
        dlt.read("SILVER_GLOBAL_OPENAQ")
        .withColumn("hour", F.date_trunc("hour", "utc_date"))
        .groupBy("city", "hour")
        .pivot("parameter")
        .agg(F.round(F.avg("value"), 4))
    )


def detect_gas_anomalies(silver_table_name):
    return (
        dlt.read(silver_table_name)
        .withColumn("hour", F.date_trunc("hour", "utc_date"))
        .groupBy("parameter", "hour", "units")
        .agg(
            F.max("value").alias("max_gas_level"),
            F.avg("value").alias("avg_gas_level")
        )
        .withColumn(
            "status",
            F.when(F.col("max_gas_level") > (F.col("avg_gas_level") * 2), "Pico Anómalo")
            .otherwise("Estable")
        )
    )

@dlt.table(name="GOLD_SF_GAS_ANOMALIES", comment ="Anomalía de gases en San Francisco Center!")
def gold_sf_anomalies():
    return detect_gas_anomalies("SILVER_SF")

@dlt.table(name="GOLD_LV_GAS_ANOMALIES", comment ="Anomalía de gases en Las Vegas Casino Center!")
def gold_lv_anomalies():
    return detect_gas_anomalies("SILVER_LV_CASINO")

@dlt.table(name="GOLD_BOSTON_GAS_ANOMALIES", comment ="Anomalía de gases en Boston Roxbury!")
def gold_boston_anomalies():
    return detect_gas_anomalies("SILVER_BOSTON_ROXBURY")