
# coding: utf-8

# In[ ]:


# Create __SparkSession__ object
# 
#     The entry point to programming Spark with the Dataset and DataFrame API.
# 
#     Used to create DataFrame, register DataFrame as tables and execute SQL over tables etc.

from pyspark.sql import SparkSession

# Import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col,udf

from pyspark.ml import Pipeline, PipelineModel

spark = SparkSession.builder.appName("Kafka Spark Structured Streaming").config("spark.master", "local").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Loading the pipeline model
model = PipelineModel.load("file:////home/2573B55/uberModel")

print(model)

# Reading the messsages from the kafka topic and creating a data frame
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","c.insofe.edu.in:9092").option("subscribe","2573B55UberTopic").option("startingOffsets", "earliest").load()

df.printSchema()

# Converting the columns to appropriate data types
df = df.select(col("value").cast("string"), col("timestamp"))

# Splitting the value column based on comma delimiter and creating the columns Date,Latitude,Longitude, ID and Status columns
df = df.withColumn('Date', split(df.value, ",")[0])
df = df.withColumn('Latitude', split(df.value, ",")[1])
df = df.withColumn('Longitude', split(df.value, ",")[2])
df = df.withColumn('ID', split(df.value, ",")[3])
df = df.withColumn('Status', split(df.value, ",")[4])
df = df.select("Date","Latitude","Longitude", "ID","Status","timestamp")

# Converting the Latitude and Longitude columns to double data type
df = df.withColumn("Latitude", df["Latitude"].cast("double"))
df = df.withColumn("Longitude", df["Longitude"].cast("double"))

df.printSchema()
# Filtering the records with status Active ("A")
df = df[df.Status == '"A"']

# Predicting the cluster numbers on the streaming data
test_predictions_lr = model.transform(df)

# Writing the predictions to a permanent storage
# Spark structured streaming only supports "parquet" format for now.
# The output mode should be in append mode and also the checkpoint location needs to be mentioned.
query = test_predictions_lr.writeStream.format("parquet").outputMode("append").option("truncate","false").option("path", "file:///home/2573B55/Uber_Use_Case/results/output").option("checkpointLocation", "file:///home/2573B55/Uber_Use_Case/results/outputCP").start()

#Start running the query that prints the running counts to the console
query = test_predictions_lr.writeStream.format("console").outputMode("append").option("truncate","false").start()

query.awaitTermination()