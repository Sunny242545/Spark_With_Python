from pyspark.sql import SparkSession

#Creating a Spark Session
print("Creating Spark Session....")
spark: SparkSession =(
    SparkSession.builder
    .appName("groupby_aggregates")
    .getOrCreate()
)















print("=========================================================================")
print("***********************Execution Complted********************************")
print("=========================================================================")
spark.stop()