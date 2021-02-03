import argparse
import itertools
import copy
import pandas as pd
import csv
import numpy as np
from datetime import datetime, timedelta

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType, DateType
import pyspark.sql.functions as functions  
from pyspark.sql.window import Window

YEAR = 2021

MIN_TEMPERATURE = 290
MAX_TEMPERATURE = 300

MIN_HUMIDITY = 60
MAX_HUMIDITY = 90

MIN_WIND = 0
MAX_WIND = 2

DAYS = 21

def subtract_date(start_date, days_to_subtract):
    return start_date + timedelta(days_to_subtract)


spark = SparkSession.builder.appName("Project").getOrCreate()
df = spark.read.csv("./dataset/data.csv", header=True)

spark_df = df.groupBy("city", functions.year("datetime").alias("Year"), functions.dayofyear("datetime").alias("DayOfYear")) \
    .agg(functions.mean("temperature").alias("Avg(Temperature)"), \
    functions.mean("humidity").alias("Avg(Humidity)"), \
    functions.mean("pressure").alias("Avg(Pressure)"), \
    functions.mean("wind_speed").alias("Avg(WindSpeed)"), \
    functions.mean("wind_direction").alias("Avg(WindDirection)")) \
    .orderBy("city", "Year", 'DayOfYear')

spark_df = spark_df.groupBy("city", "DayOfYear") \
    .agg(functions.mean("Avg(Temperature)").alias("Avg5(Temperature)"), \
    functions.mean("Avg(Humidity)").alias("Avg5(Humidity)"), \
    functions.mean("Avg(WindSpeed)").alias("Avg5(WindSpeed)")) \
    .orderBy("city", 'DayOfYear')


if DAYS % 2 == 1:
    lower_bound = int(DAYS / 2)    
else:
    lower_bound = int(DAYS / 2) - 1

upper_bound = int(DAYS / 2)

window = Window.partitionBy("city").orderBy("DayOfYear").rowsBetween(-lower_bound, upper_bound)

spark_df = spark_df.withColumn("temperature_ind", ((spark_df['Avg5(Temperature)'] > MIN_TEMPERATURE) & (spark_df['Avg5(Temperature)'] < MAX_TEMPERATURE)).cast("int"))
spark_df = spark_df.withColumn("humidity_ind", ((spark_df['Avg5(Humidity)'] > MIN_HUMIDITY) & (spark_df['Avg5(Humidity)'] < MAX_HUMIDITY)).cast("int"))
spark_df = spark_df.withColumn("wind_ind", ((spark_df['Avg5(WindSpeed)'] > MIN_WIND) & (spark_df['Avg5(WindSpeed)'] < MAX_WIND)).cast("int"))

spark_df = spark_df.withColumn("temperature_sub", functions.sum("temperature_ind").over(window))
spark_df = spark_df.withColumn("humidity_sub", functions.sum("humidity_ind").over(window))
spark_df = spark_df.withColumn("wind_sub", functions.sum("wind_ind").over(window))

spark_df = spark_df.filter((spark_df['temperature_sub'] == DAYS) & (spark_df['humidity_sub'] == DAYS) & (spark_df['wind_sub'] == DAYS))

start = datetime(YEAR, 1, 1)

subtract_date_udf = functions.udf(subtract_date, DateType())
spark_df = spark_df.withColumn('start_date', subtract_date_udf(functions.lit(start), spark_df['DayOfYear'] - lower_bound - 1))

spark_df.toPandas().to_csv('output5.csv', columns=["city", "start_date"])