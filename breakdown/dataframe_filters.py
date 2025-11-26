from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#creating a spark session
print("Creating a spark session:")

spark: SparkSession =(
    SparkSession.builder
    .appName("DataFrame_Filters")
    .getOrCreate()
)

#Reading data from CSV to Dataframe
print("Reading data from csv to a Dataframe:")
df = spark.read.csv('employes.csv', header = True, inferSchema = True)
print("This is the original data from csv file:")
df.show()


#simple filter that filters the data with salary >=80000
print("Filtered dataframe with salary more than or equal to 80000:")
filtered_df = df.filter("Salary>=80000") #filter using sql string
filtered_df.show()

filtered_df = df.filter(df.Salary>=80000) #filter using column instance
filtered_df.show()

filtered_df = df.filter(df['Salary']>=80000) 
filtered_df.show()

print("Filtering the data using .where instead of .filter")
filtered_df = df.where(df.Salary>=80000)
filtered_df.show()


#Filtering the data using multiple conditions
#Salary >50000 and Experience >=4 years
print("Filretring the data with Salary >50000 and Experience >= 4 Years:")
filtered_df = df.filter((df.Salary>50000) & (df.Experience>=4))
filtered_df.show()

filtered_df = df.where((df.Salary>50000) & (df.Experience>=4)) # multiple condition filtering using where
filtered_df.show()


#Filtering using col function 
print("Filtered Data using col function as Salary>50000 and Experience >=4:")
filtered_df = df.filter((F.col('Salary')>50000) & (F.col('Experience')>=4)) 
filtered_df.show()












print("=========================================================================")
print("***********************Execution Complted********************************")
print("=========================================================================")
spark.stop()