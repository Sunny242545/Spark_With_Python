import pyspark # type: ignore
import pandas as pd # type: ignore
from pyspark.sql import SparkSession # type: ignore

#create a spark session
spark: SparkSession =(
    SparkSession.builder
    .appName('PlayingSpark')
    .getOrCreate()
)
#read a csv file
df = spark.read.csv('persons.csv', header = True, inferSchema = True)
df.printSchema()
df.show()

spark.stop()

