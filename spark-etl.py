import pyspark # type: ignore
import pandas as pd # type: ignore
from pyspark.sql import SparkSession # type: ignore

spark: SparkSession =(
    SparkSession.builder
    .appName('PlayingSpark')
    .getOrCreate()
)

df = spark.read.csv('persons.csv', header = True)
df.printSchema()
df.show()

spark.stop()

