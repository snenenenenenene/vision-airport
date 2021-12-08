from numpy import string
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import pyarrow.parquet as pq

spark = SparkSession.builder.appName("vision-airport").getOrCreate()

DATADIR = "./data"


# Vertrek

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

vertrek_df.filter("Vracht != null").show()


# Aankomst

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

aankomst_df.show()


# Planning

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

planning_df.show()
