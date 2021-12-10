from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from dotenv import load_dotenv
import os

load_dotenv()

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
ENDPOINT = os.environ.get("ENDPOINT")
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"

spark = SparkSession.builder.master("local").appName("visie-luchthaven").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("fs.s3a.access.key", ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", ENDPOINT)
spark.conf.set("spark.sql.caseSensitive","true")

DATADIR = "./data/"
BUCKET = "s3a://ap-project-big-data-2021/BD-01/cleansed/"


# Airport ================================================================================================

airport_schema = StructType([
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

airport_df = spark.read.csv(
    DATADIR + "export_luchthavens.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=airport_schema
)

airport_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "airport.parquet")

# Banen ==================================================================================================

banen_schema = StructType([
    StructField("Baannummer", IntegerType(), True),
    StructField("Code",StringType(),True),
    StructField("Naam", StringType(),True),
    StructField("Lengte", IntegerType(), True),
  ])

banen_df = spark.read.csv(
    DATADIR + "export_banen.csv", 
    header=True,
    sep=';',
    schema= banen_schema
)

banen_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "banen.parquet")

# Customers ==============================================================================================

customers_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('operation', FloatType(), True),
    StructField('facilities', FloatType(), True),
    StructField('shops', FloatType(), True),
])

customers_df = spark.read.csv(
    DATADIR + "export_klant.csv", 
    header=True,
    sep=";",
    multiLine=True,
    schema=customers_schema
)

customers_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "customers.parquet")

# Maatschappij ===========================================================================================

maatschappijen_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("IATA", StringType(), True),
    StructField("ICAO", StringType(), True),
  ])

maatschappijen_df = spark.read.csv(
    DATADIR + "export_maatschappijen.txt", 
    header=True,
    sep='\t',
    schema=maatschappijen_schema
)

maatschappijen_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "maatschappijen.parquet")

# Planning ===============================================================================================

planning_schema = StructType([
    StructField("Vluchtnr", StringType(), False),
    StructField("Airlinecode", StringType(), True),
    StructField("Destcode", StringType(), True),
    StructField("Planterminal", StringType(), True),
    StructField("Plangate", StringType(), True),
    StructField("Plantijd", StringType(), True)
  ])

planning_df = spark.read.csv(
    DATADIR + "export_planning.txt",
    header=True,
    sep='\t',
    schema=planning_schema
)

planning_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "planning.parquet")

# Vliegtuigtype ==========================================================================================



# Vliegtuig ==============================================================================================

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
    DATADIR + "export_vliegtuigtype.csv", 
    header=True,
    sep=';',
    schema=vliegtuig_type_schema
)

vliegtuig_schema = StructType([
    StructField("Airlinecode", StringType(),True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Vliegtuigtype", StringType(),True),
    StructField("Bouwjaar", IntegerType(), True)
  ])

vliegtuig_df = spark.read.csv(
    DATADIR + "export_vliegtuig.txt", 
    header=True,
    sep='\t',
    schema=vliegtuig_schema
)

vliegtuig_df = vliegtuig_df\
    .join(vliegtuig_type, vliegtuig_df.Vliegtuigtype == vliegtuig_type.IATA)\
    .drop("Vliegtuigtype")

vliegtuig_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "vliegtuig.parquet")

# Vlucht =================================================================================================

vlucht_schema = StructType([
    StructField("Vluchtid", IntegerType(), True),
    StructField("Vluchtnr",StringType(),True),
    StructField("Airlinecode", StringType(),True),
    StructField("Destcode", StringType(), True),
    StructField("Vliegtuigcode", StringType(), True),
    StructField("Datum", DateType(), True)
  ])

vlucht_df = spark.read.csv(
    DATADIR + "export_vlucht.txt", 
    header=True,
    sep='\t',
    schema=vlucht_schema
)

vlucht_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "vlucht.parquet")


# Weather ================================================================================================

weather_schema = StructType([
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
    StructField('TXH', IntegerType(), True),
    StructField('T10N', IntegerType(), True),
    StructField('T10NH', IntegerType(), True),
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

weather_df = spark.read.csv(
    DATADIR + "export_weer.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=weather_schema
)

weather_df.write\
    .mode("overwrite")\
    .parquet(BUCKET + "weather.parquet")


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
    DATADIR + "export_vertrek.txt",
    header=True,
    sep='\t',
    schema=vertrek_schema
)

vertrek_df = vertrek_df.withColumn("Vertrekjaar", F.date_format(vertrek_df.Vertrektijd, "yyyy")).withColumn("Vertrekmaand", F.date_format(vertrek_df.Vertrektijd, "MM"))

vertrek_df.write\
    .partitionBy("Vertrekjaar", "Vertrekmaand")\
    .mode("overwrite")\
    .parquet(BUCKET + "vertrek.parquet")

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
    DATADIR + "export_aankomst.txt",
    header=True,
    sep='\t',
    schema=aankomst_schema
)

aankomst_df = aankomst_df.withColumn("Aankomstjaar", F.date_format(aankomst_df.Aankomsttijd, "yyyy")).withColumn("Aankomstmaand", F.date_format(aankomst_df.Aankomsttijd, "MM"))

aankomst_df.write\
    .partitionBy("Aankomstjaar", "Aankomstmaand")\
    .mode("overwrite")\
    .parquet(BUCKET + "aankomst.parquet")
