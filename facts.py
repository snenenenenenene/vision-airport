from logging import setLoggerClass
import os

import pyspark.sql.functions as F
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

load_dotenv()

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
ENDPOINT = os.environ.get("ENDPOINT")
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"

spark = SparkSession.builder.master("local").appName("visie-luchthaven").getOrCreate()

spark.conf.set("fs.s3a.access.key", ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", ENDPOINT)
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.caseSensitive","true")

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


DATADIR = "./data/"
BUCKET = "s3a://ap-project-big-data-2021/BD-01/factstables/"


# DIMENSIONS =============================================================================================

# Luchthaven ================================================================================================

dim_luchthaven_schema = StructType([
    StructField('Airport', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('IATA', StringType(), True),
    StructField('ICAO', StringType(), True),
    StructField('Lat', FloatType(), True),
    StructField('Lon', FloatType(), True),
    StructField('Alt', StringType(), True),
    StructField('TZ', FloatType(), True),
    StructField('DST', StringType(), True),
    StructField('Tz', StringType(), True),
])

dim_luchthaven_df = spark.read.csv(
    DATADIR + "export_luchthavens.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=dim_luchthaven_schema
)

dim_luchthaven_df = dim_luchthaven_df.withColumnRenamed("Tz", "timezone")\


# Banen ==================================================================================================

dim_banen_schema = StructType([
    StructField("Baannummer", IntegerType(), True),
    StructField("Code",StringType(),True),
    StructField("Naam", StringType(),True),
    StructField("Lengte", IntegerType(), True),
  ])

dim_banen_df = spark.read.csv(
    DATADIR + "export_banen.csv", 
    header=True,
    sep=';',
    schema= dim_banen_schema
)

# Klanten ==============================================================================================

dim_klanten_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('operation', FloatType(), True),
    StructField('facilities', FloatType(), True),
    StructField('shops', FloatType(), True),
])

dim_klanten_df = spark.read.csv(
    DATADIR + "export_klant.csv", 
    header=True,
    sep=";",
    multiLine=True,
    schema=dim_klanten_schema
)

# Maatschappij ===========================================================================================

dim_maatschappijen_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("IATA", StringType(), True),
    StructField("ICAO", StringType(), True),
  ])

dim_maatschappijen_df = spark.read.csv(
    DATADIR + "export_maatschappijen.txt", 
    header=True,
    sep='\t',
    schema=dim_maatschappijen_schema
)

# Planning ===============================================================================================

dim_planning_schema = StructType([
    StructField("vluchtnr", StringType(), False),
    StructField("Airlinecode", StringType(), True),
    StructField("Destcode", StringType(), True),
    StructField("Planterminal", StringType(), True),
    StructField("Plangate", StringType(), True),
    StructField("Plantijd", StringType(), True)
  ])

dim_planning_df = spark.read.csv(
    DATADIR + "export_planning.txt",
    header=True,
    sep='\t',
    schema=dim_planning_schema
)

dim_planning_df = dim_planning_df.withColumn('Plantijd',F.date_format(F.to_timestamp("Plantijd", "hh:mm a"), "HH:MM:SS"))


# Vliegtuig ==============================================================================================

dim_vliegtuig_type_schema = StructType([
    StructField("IATA", StringType(), True),
    StructField("ICAO",StringType(),True),
    StructField("Merk", StringType(),True),
    StructField("Type", StringType(), True),
    StructField("Wake", StringType(), True),
    StructField("Cat", StringType(), True),
    StructField("Capaciteit", IntegerType(), True),
    StructField("Vracht", IntegerType(), True)
  ])

dim_vliegtuig_type_df = spark.read.csv(
    DATADIR + "export_vliegtuigtype.csv", 
    header=True,
    sep=';',
    schema=dim_vliegtuig_type_schema
)

dim_vliegtuig_schema = StructType([
    StructField("Airlinecode", StringType(),True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Vliegtuigtype", StringType(),True),
    StructField("Bouwjaar", IntegerType(), True)
  ])

dim_vliegtuig_df = spark.read.csv(
    DATADIR + "export_vliegtuig.txt", 
    header=True,
    sep='\t',
    schema=dim_vliegtuig_schema
)

dim_vliegtuig_df = dim_vliegtuig_df\
    .join(dim_vliegtuig_type_df, dim_vliegtuig_df.Vliegtuigtype == dim_vliegtuig_type_df.IATA)\
    .withColumnRenamed("Vracht", "vracht_vliegtuigtype")\


# Vlucht =================================================================================================

dim_vlucht_schema = StructType([
    StructField("Vluchtid", IntegerType(), True),
    StructField("Vluchtnr",StringType(),True),
    StructField("Airlinecode", StringType(),True),
    StructField("Destcode", StringType(), True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Datum", DateType(), True)
  ])

dim_vlucht_df = spark.read.csv(
    DATADIR + "export_vlucht.txt", 
    header=True,
    sep='\t',
    schema=dim_vlucht_schema
)

# Weather ================================================================================================

dim_weather_schema = StructType([
    StructField('date', DateType(), True),    
    StructField('DDVEC', IntegerType(), True),
    StructField('FHVEC', IntegerType(), True),
    StructField('FG', IntegerType(), True),
    StructField('FHX', IntegerType(), True),
    StructField('FHXH', IntegerType(), True),
    StructField('FHN', IntegerType(), True),
    StructField('FHNH', IntegerType(), True),
    StructField('FXX', IntegerType(), True),
    StructField('FXXH', IntegerType(), True),
    StructField('TG', IntegerType(), True),
    StructField('TN', IntegerType(), True),
    StructField('TNH', IntegerType(), True),
    StructField('TX', IntegerType(), True),
    StructField('SQ', IntegerType(), True),
    StructField('SP', IntegerType(), True),
    StructField('Q', IntegerType(), True),
    StructField('DR', IntegerType(), True),
    StructField('RH', IntegerType(), True),
    StructField('RHX', IntegerType(), True),
    StructField('RHXH', IntegerType(), True),
    StructField('PG', IntegerType(), True),
    StructField('PX', IntegerType(), True),
    StructField('PXH', IntegerType(), True),
    StructField('PN', IntegerType(), True),
    StructField('PNH', IntegerType(), True),
    StructField('VVN', IntegerType(), True),
    StructField('VVNH', IntegerType(), True),
    StructField('VVX', IntegerType(), True),
    StructField('VVXH', IntegerType(), True),
    StructField('NG', IntegerType(), True),
    StructField('UG', IntegerType(), True),
    StructField('UX', IntegerType(), True),
    StructField('UXH', IntegerType(), True),
    StructField('UN', IntegerType(), True),
    StructField('UNH', IntegerType(), True),
    StructField('EV2', IntegerType(), True),
])

dim_weather_df = spark.read.csv(
    DATADIR + "export_weer.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=dim_weather_schema
)

dim_weather_df = dim_weather_df.select("date", "FHVEC", "TG", "RH", "VVX")\
        .withColumnRenamed("FHVEC", "windsnelheid_gem")\
        .withColumnRenamed("TG", "temp_gem")\
        .withColumnRenamed("RH", "neerslag_som")\
        .withColumnRenamed("VVX", "opgetreden_zicht_max")

# FACTS ===========================================================================================

# Vertrek =========================================================================================

facts_vertrek_schema = StructType([
    StructField("vluchtid", IntegerType(), False),
    StructField("vliegtuigcode", StringType(), False),
    StructField("terminal", StringType(),True),
    StructField("gate", StringType(), True),
    StructField("baan", ShortType(), True),
    StructField("bezetting", IntegerType(), True),
    StructField("vracht", IntegerType(), True),
    StructField("vertrektijd", TimestampType(), True)
  ])

facts_vertrek_df = spark.read.csv(
    DATADIR + "export_vertrek.txt",
    header=True,
    sep='\t',
    schema=facts_vertrek_schema
)

print(f'\nVERTREK BEFORE {facts_vertrek_df.count()}==================================================================\n')

facts_vertrek_df = facts_vertrek_df\
    .join(dim_vlucht_df, facts_vertrek_df.vluchtid == dim_vlucht_df.Vluchtid).drop("Vluchtid", "Destcode")\
    .join(dim_klanten_df, facts_vertrek_df.vluchtid == dim_klanten_df.id, "fullouter").drop("id")\
    .join(dim_banen_df, facts_vertrek_df.baan == dim_banen_df.Baannummer).drop("Baannummer")

facts_vertrek_df = facts_vertrek_df\
    .join(dim_planning_df, facts_vertrek_df.Vluchtnr == dim_planning_df.vluchtnr, "fullouter").drop("vluchtnr", "Airlinecode")\
    .join(dim_vliegtuig_df, facts_vertrek_df.vliegtuigcode == dim_vliegtuig_df.Vliegtuigcode).drop("Vliegtuigcode")\
    .join(dim_weather_df, facts_vertrek_df.Datum == dim_weather_df.date).drop("date")

facts_vertrek_df = facts_vertrek_df\
    .join(dim_luchthaven_df, facts_vertrek_df.Destcode == dim_luchthaven_df.IATA)\
    .join(dim_maatschappijen_df, facts_vertrek_df.Airlinecode == dim_maatschappijen_df.IATA).drop("IATA", "ICAO")



print(f'\nVERTREK AFTER {facts_vertrek_df.count()}==================================================================\n')

def getVertraging(actual, planned):
    if actual is None or planned is None:
        return None
    planned = datetime.strptime(planned, "%H:%M:%S").time()
    actual = actual.time()
    return (actual.hour - planned.hour)*60 + (actual.minute - planned.minute)

vertraging = F.udf(lambda x, y: getVertraging(x, y), IntegerType())

facts_vertrek_df = facts_vertrek_df.withColumn("vertraging_min", vertraging(F.col("vertrektijd"), F.col("Plantijd")))

facts_vertrek_df.show()

facts_vertrek_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "vertrek.parquet")

# Aankomst ===========================================================================================

facts_aankomst_schema = StructType([
    StructField("vluchtid", IntegerType(), False),
    StructField("vliegtuigcode", StringType(), False),
    StructField("terminal", StringType(), True),
    StructField("gate", StringType(), True),
    StructField("baan", ShortType(), True),
    StructField("bezetting", IntegerType(), True),
    StructField("vracht", IntegerType(), True),
    StructField("aankomsttijd", TimestampType(), True),
  ])

facts_aankomst_df = spark.read.csv(
    DATADIR + "export_aankomst.txt",
    header=True,
    sep='\t',
    schema=facts_aankomst_schema
)


print(f'\n AANKOMST BEFORE {facts_aankomst_df.count()}==================================================================\n')

facts_aankomst_df = facts_aankomst_df.dropna(subset="aankomsttijd")
facts_aankomst_df = facts_aankomst_df\
    .join(dim_vlucht_df, facts_aankomst_df.vluchtid == dim_vlucht_df.Vluchtid).drop("Vluchtid", "Destcode")\
    .join(dim_banen_df, facts_aankomst_df.baan == dim_banen_df.Baannummer).drop("Baannummer")

facts_aankomst_df = facts_aankomst_df\
    .join(dim_planning_df, facts_aankomst_df.Vluchtnr == dim_planning_df.vluchtnr, "fullouter").drop("vluchtnr", "Airlinecode")\
    .join(dim_vliegtuig_df, facts_aankomst_df.vliegtuigcode == dim_vliegtuig_df.Vliegtuigcode, "fullouter").drop("Vliegtuigcode")\
    .join(dim_weather_df, facts_aankomst_df.Datum == dim_weather_df.date, "fullouter").drop("date")

facts_aankomst_df = facts_aankomst_df\
    .join(dim_luchthaven_df, facts_aankomst_df.Destcode == dim_luchthaven_df.IATA)\
    .join(dim_maatschappijen_df, facts_aankomst_df.Airlinecode == dim_maatschappijen_df.IATA).drop("IATA", "ICAO")


facts_aankomst_df = facts_aankomst_df.withColumn("vertraging_min", vertraging(F.col("aankomsttijd"), F.col("Plantijd")))


print(f'\n AANKOMST AFTER {facts_aankomst_df.count()}==================================================================\n')

facts_aankomst_df.show()

facts_aankomst_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "aankomst.parquet")
