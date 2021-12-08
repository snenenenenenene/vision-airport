from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"

spark = SparkSession.builder.master("local").appName("visie-luchthaven").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("fs.s3a.access.key", "AKIATUJ2TY2HH3TJP2X6")
spark.conf.set("fs.s3a.secret.key", "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")
spark.conf.set("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")

DATADIR = "./data"
BUCKET = "s3a://ap-project-big-data-2021/BD-01/cleansed/"

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

vertrek_df.write.parquet(BUCKET + "vertrek.parquet")

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

aankomst_df.write.parquet(BUCKET + "aankomst.parquet")

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

planning_df.write.parquet(BUCKET + "planning.parquet")
