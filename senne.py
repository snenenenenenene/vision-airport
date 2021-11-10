import boto3
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import col, desc, asc, lit
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from matplotlib.pyplot import rc

sc = SparkContext("local").getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
plt.style.use('ggplot')

# AIRPORT DATAFRAME

airport_schema = StructType([
    StructField('airport', StringType(), True),
    StructField('city', StringType(), True),
    StructField('country', StringType(), True),
    StructField('IATA', StringType(), True),
    StructField('ICAO', StringType(), True),
    StructField('lat', FloatType(), True),
    StructField('lon', FloatType(), True),
    StructField('alt', StringType(), True),
    StructField('tz', StringType(), True),
    StructField('dst', StringType(), True),
    StructField('timezone', StringType(), True),
])


airport_df = spark.read.csv(
    "./data/export_luchthavens.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=airport_schema
)

# CUSTOMERS DATAFRAME

customers_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('operation', FloatType(), True),
    StructField('facilities', FloatType(), True),
    StructField('shops', FloatType(), True),
])


customers_df = spark.read.csv(
    "./data/export_klant.csv", 
    header=True,
    sep=";",
    multiLine=True,
    schema=customers_schema
)

# WEATHER DATAFRAME

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
    "./data/export_weer.txt", 
    header=True,
    sep="\t",
    multiLine=True,
    schema=weather_schema
)


# airport_df.show(3, False)
# customers_df.show(3, False)
weather_df.show(3, False)