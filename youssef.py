import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("vision-airport").getOrCreate()

banen_schema = StructType([
    StructField("Baannummer", IntegerType(), True),
    StructField("Code",StringType(),True),
    StructField("Naam", StringType(),True),
    StructField("Lengte", IntegerType(), True),
  ])

maatschappijen_schema = StructType([
    StructField("Name", StringType(),True),
    StructField("IATA", StringType(), True),
    StructField("ICAO",StringType(),True),
  ])

banen_df = spark.read.csv(
    "C:\export_banen.csv", 
    header=True,
    sep=';',
    schema= banen_schema
)

banen_df.write.parquet("banen.parquet")

maatschappijen_df = spark.read.csv(
    "C:\export_maatschappijen.txt", 
    header=True,
    sep='\t',
    schema=maatschappijen_schema
)

maatschappijen_df.write.parquet("maatschappijen.parquet")
