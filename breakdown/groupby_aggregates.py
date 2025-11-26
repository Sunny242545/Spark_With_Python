from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
print("Checking the Total amount of funds used as salary for each department")
filtered_df = (df.groupBy('Department')
               .agg(F.sum('Salary').alias('TotalSalary'))
               .orderBy('TotalSalary', ascending = False))
filtered_df.show()


print("Checking num of employes present on each deppartment whose Experience more than or equal to 4:")
filtered_df = df.filter(df.Experience>=4).groupBy('Department').agg(F.count('Salary').alias('Total_Employes'))
filtered_df.show()

#Grouop by always collapse the rows,
#if you dont want to collapse the rows and still need the aggregares!
#Use repartition with WithColumn(window function)instead of Group by
print("Average Salary for each department using window function:")
windowSpec = Window.partitionBy('Department')

filtered_df = df.withColumn('Avg_Salary', F.mean('Salary').over(windowSpec))
filtered_df.show()

print("Culumative Salary order by Age and Name:")
windowSpec = Window.orderBy('Age','Name')

filtered_df = df.withColumn('Cumulative_Salary', F.sum('Salary').over(windowSpec))
filtered_df.show()






print("=========================================================================")
print("***********************Execution Complted********************************")
print("=========================================================================")
spark.stop()