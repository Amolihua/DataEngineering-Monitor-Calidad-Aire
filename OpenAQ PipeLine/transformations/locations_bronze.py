import dlt
from pyspark.sql import functions as F

catalog = "openaq"
schema = "livestreamdata"
base_path = f"/Volumes/{catalog}/{schema}/raw_data/openaq_v3"

def create_bronze_table(folder_name):
    source_path = f"{base_path}/{folder_name}/*.json"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    
        # Explotamos el array results y agregamos metadata de auditoría
        .select(
            F.explode("results").alias("r"), 
            F.current_timestamp().alias("ingested_at"),
            F.col("_metadata.file_path").alias("source_file")
        )
        # Extraemos los campos exactos validados en el notebook exploratorio
        .select(
            F.lit(folder_name).alias("city"),
            F.col("r.parameter.name").alias("parameter"),
            F.col("r.parameter.units").alias("units"),
            F.col("r.value").alias("value"),
            F.col("r.period.datetimeTo.utc").alias("utc_date"),
            "ingested_at",
            "source_file"
        )
    )

@dlt.table(name="BRONZE_SF", comment="Datos crudos y aplanados de San Francisco")
def bronze_sf():
    return create_bronze_table("san_francisco")

@dlt.table(name="BRONZE_LV_CASINO", comment="Datos crudos y aplanados de Las Vegas")
def bronze_lv():
    return create_bronze_table("las_vegas_casino_center")

@dlt.table(name="BRONZE_BOSTON_ROXBURY", comment="Datos crudos y aplanados de Boston")
def bronze_boston():
    return create_bronze_table("boston_roxbury")