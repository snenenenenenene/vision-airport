from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("vision-airport").getOrCreate()

DATADIR = "./data"

# Vertrek =========================================================================================

vertrek_schema = StructType([
    StructField("Vluchtid", IntegerType(), False),
    StructField("Vliegtuigcode",StringType(),True),
    StructField("Terminal", StringType(),True),
    StructField("Gate", StringType(), True),
    StructField("Baan", ShortType(), True),
    StructField("Bezetting", IntegerType(), True),
    StructField("Vracht", IntegerType(), True),
    StructField("Vertrektijd", TimestampType(), True)
  ])

vertrek_df = spark.read.csv(
    DATADIR + "/export_vertrek.txt", 
    header=True,
    sep='\t',
    schema=vertrek_schema
)

vertrek_df = vertrek_df.dropna()
vertrek_df.write.parquet("aws/vertrek.parquet")

# Aankomst ===========================================================================================

aankomst_schema = StructType([
    StructField("Vluchtid", IntegerType(), False),
    StructField("Vliegtuigcode",StringType(),True),
    StructField("Terminal", StringType(), True),
    StructField("Gate", StringType(), True),
    StructField("Baan", ShortType(), True),
    StructField("Bezetting", IntegerType(), True),
    StructField("Vracht", IntegerType(), True),
    StructField("Aankomsttijd", TimestampType(), True)
  ])

aankomst_df = spark.read.csv(
    DATADIR + "/export_aankomst.txt", 
    header=True,
    sep='\t',
    schema=aankomst_schema
)

aankomst_df = aankomst_df.dropna()
aankomst_df.write.parquet("aws/aankomst.parquet")

# Planning ===============================================================================================

planning_schema = StructType([
    StructField("Vluchtnr", IntegerType(), False),
    StructField("Airlinecode", StringType(), True),
    StructField("Destcode", StringType(), True),
    StructField("Planterminal", StringType(), True),
    StructField("Plangate", StringType(), True),
    StructField("Plantijd", StringType(), True)
  ])

planning_df = spark.read.csv(
    DATADIR + "/export_planning.txt", 
    header=True,
    sep='\t',
    schema=planning_schema
)

planning_df = planning_df.dropna()
planning_df.write.parquet("aws/planning.parquet")
