import dlt
from pyspark.sql import functions as F

def clean_bronze_stream(bronze_table_name):
    return (
        dlt.read_stream(bronze_table_name)
        .withColumn("utc_date", F.col("utc_date").cast("timestamp"))
        .filter(F.col("value").isNotNull() & (F.col("value") >= 0))
    )

@dlt.table(name="SILVER_SF")
def silver_sf():
    return clean_bronze_stream("BRONZE_SF")

@dlt.table(name="SILVER_LV_CASINO")
def silver_lv():
    return clean_bronze_stream("BRONZE_LV_CASINO")

@dlt.table(name="SILVER_BOSTON_ROXBURY")
def silver_boston():
    return clean_bronze_stream("BRONZE_BOSTON_ROXBURY")

@dlt.table(name="SILVER_GLOBAL_OPENAQ")
def silver_global():
    df_sf = dlt.read_stream("SILVER_SF")
    df_lv = dlt.read_stream("SILVER_LV_CASINO")
    df_boston = dlt.read_stream("SILVER_BOSTON_ROXBURY")
    
    return df_sf.union(df_lv).union(df_boston)