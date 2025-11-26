from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Creating a Spark Session
print("Creating Spark Session....")
spark: SparkSession =(
    SparkSession.builder
    .appName("groupby_aggregates")
    .getOrCreate()
)

#Reading the CSV file into a Dataframe
print("Reading a CSV file into Dataframe:")
df = spark.read.csv('employe_with_department.csv', header = True, inferSchema = True)
print("This is the original data of a dataframe we got from CSV")
df.show()

#Check the total salary for each department
filtered_df = df.groupBy('Department').agg(F.sum('Salary').alias('TotalSalary')).orderBy('TotalSalary', ascending = False)
filtered_df.show()














print("=========================================================================")
print("***********************Execution Complted********************************")
print("=========================================================================")
spark.stop()