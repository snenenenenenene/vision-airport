import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.appName("vision-airport").getOrCreate()

# vlucht

vlucht_schema = StructType([
    StructField("Vluchtid", IntegerType(), True),
    StructField("Vluchtnr",StringType(),True),
    StructField("Airlinecode", StringType(),True),
    StructField("Destcode", StringType(), True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Datum", DateType(), True)
  ])

vlucht_df = spark.read.csv(
    "../data/export_vlucht.txt", 
    header=True,
    sep='\t',
    schema=vlucht_schema
)

# vlucht_df.write.parquet("vlucht.parquet")
vlucht_df.show()

#vliegtuig

vliegtuig_schema = StructType([
    StructField("Airlinecode", StringType(),True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Vliegtuigtype",StringType(),True),
    StructField("Bouwjaar", IntegerType(), True)
  ])

vliegtuig_df = spark.read.csv(
    "../data/export_vliegtuig.txt", 
    header=True,
    sep='\t',
    schema=vliegtuig_schema
)
vliegtuig_df.show()
# vliegtuig_df.write.parquet("vliegtuig.parquet")

#VliegtuigType

vliegtuig_type_schema = StructType([
    StructField("IATA", StringType(), True),
    StructField("ICAO",StringType(),True),
    StructField("Merk", StringType(),True),
    StructField("Type", StringType(), True),
    StructField("Wake", StringType(), True),
    StructField("Cat", StringType(), True),
    StructField("Capaciteit", IntegerType(), True),
    StructField("Vracht", IntegerType(), True)
  ])

vliegtuig_type = spark.read.csv(
    "../data/export_vliegtuigtype.csv", 
    header=True,
    sep=';',
    schema=vliegtuig_type_schema
)
vliegtuig_type.show()
# vliegtuig_type.write.parquet("vliegtuig_type.parquet")

# import pyarrow.parquet as pq
