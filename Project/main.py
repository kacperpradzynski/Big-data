from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import year, dayofyear, mean, round
from pyspark.ml.clustering import KMeans
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler, MinMaxScaler

FEATURES_COL = ["DayOfYear", "Avg(Temperature)", "Avg(Humidity)", "Avg(Pressure)", "Avg(WindSpeed)"]

spark = SparkSession.builder.appName("Project").getOrCreate()
df = spark.read.csv("./data.csv", header=True)
spark_df = df.filter((df.city == "Las Vegas") & (year("datetime") == 2016 )) \
    .groupBy(dayofyear("datetime").alias("DayOfYear")) \
    .agg(mean("temperature").alias("Avg(Temperature)"), \
    mean("humidity").alias("Avg(Humidity)"), \
    mean("pressure").alias("Avg(Pressure)"), \
    mean("wind_speed").alias("Avg(WindSpeed)"), \
    mean("wind_direction").alias("Avg(WindDirection)")) \
    .orderBy('DayOfYear')

vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
df_kmeans = vecAssembler.transform(spark_df).select('DayOfYear', 'features')
scaler = MinMaxScaler(inputCol="features",\
         outputCol="scaledFeatures")
scalerModel =  scaler.fit(df_kmeans.select("features"))
scaledData = scalerModel.transform(df_kmeans)
scaledData.show()

k = 5
kmeans = KMeans().setK(k).setSeed(1337).setFeaturesCol("scaledFeatures")
model = kmeans.fit(scaledData)
centers = model.clusterCenters()

print("Cluster Centers: ")
for center in centers:
    print(center)

transformed = model.transform(scaledData).select('DayOfYear', 'prediction')
rows = transformed.collect()

sqlContext = SQLContext(spark)
df_pred = sqlContext.createDataFrame(rows)
df_pred.show()

df_pred = df_pred.join(spark_df, 'DayOfYear')
df_pred.show()

pddf_pred = df_pred.toPandas()

threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
threedee.scatter(pddf_pred['Avg(Temperature)'], pddf_pred['Avg(Humidity)'], pddf_pred['DayOfYear'], c=pddf_pred.prediction)
threedee.set_xlabel("Avg(Temperature)")
threedee.set_ylabel("Avg(Humidity)")
threedee.set_zlabel("DayOfYear")
plt.show()

spark.stop()
# pandas_df =  df_pred.toPandas()
# pandas_df.plot(x ='DayOfYear', y='prediction', kind = 'scatter')
# plt.show()