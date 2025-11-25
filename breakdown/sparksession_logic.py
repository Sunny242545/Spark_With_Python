import pyspark # type: ignore
import pandas as pd # type: ignore
from pyspark.sql import SparkSession # type: ignore

#This is how we create a spark session for any project 
spark: SparkSession =(
    SparkSession.builder # This builder builds the spark session
    .appName('PlayingSpark') #appName gives the name for your application,this helps when you run multiple apps on the same cluster
    .getOrCreate() # get or create helps you to get the session if already exists or else creates
)
if spark:
    print("Spark session is created")
else:
    print("No active spark session")

spark.stop()

