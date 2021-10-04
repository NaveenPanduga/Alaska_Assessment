import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import *
from pyspark.sql import functions as F
import getpass
username = getpass.getuser()
spark = SparkSession.builder.appName(f'{username} | example code').master("yarn").getOrCreate()
airports = 'hdfs://m01.itversity.com:9000/user/itv001283/Airports.csv'

airports_df = spark.read.format('csv').option('sep', ',').option("header", "true"). \
    schema('''Origin_airport STRING,
              Destination_airport STRING,
              Origin_city STRING,
              Destination_city STRING,
              Passengers INT,
              Seats INT,
              Flights INT,
              Distance INT,
              Fly_date STRING,
              Origin_population INT,
              Destination_population INT,
              Org_airport_lat STRING,
              Org_airport_long STRING,
              Dest_airport_lat STRING,
              Dest_airport_long STRING
            '''). \
    load(airports)
airports_df.select("Origin_airport","Destination_airport","Passengers","Seats").show(10)
