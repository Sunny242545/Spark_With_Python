import pyspark # type: ignore
import pandas as pd # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import when

#create a spark session
print("Creating a spark session")
spark: SparkSession =(
    SparkSession.builder
    .appName('PlayingSpark')
    .getOrCreate()
)
#read a csv file
print("Reading a csv into a dataframe")
df = spark.read.csv('persons.csv', header = True, inferSchema = True)

print("Following is the schema of a current dataframe")
df.printSchema() # shows the schema of a dataframe

print("This is the current Dataframe")
df.show() # prints the dataframe up to a certain limit

print("Following are the columns present in a dataframe")
print(df.columns)#shows the columns in a dataframe

# print(df.select('Name')) # shows the Name column of a dataframe

# df.select('Name').show() # prints the data from Name column in a dataframe


# print(df.select(['Name','Experience'])) # shows the multiple columns of a dataframe

# df.select(['Name','Experience']).show() # prints the data from a multiple columns in a dataframe
print("Summary of a current Dataframe is as follows")
df.describe().show() # describe will show the summary of a dataframe using count, mean, standard deviation, min, max

#Adding columns in a dataframe
#spark has a special method called withColumn to add or modify the column in a dataframe
# we used when similar to case when in sql, import when from pyspark.sql.functions before using
# we can use any number of when's by adding .when with the first when condition and the otherwise inplace of else in sql
print("Adding a column called Experience Level as Senior or Junior or Mid Level")
df = df.withColumn("Experience Level",
              when (df['Experience'] < 4, 'Junior')
              .when ((df['Experience'] >= 4) & (df['Experience'] <= 6),'Mid Level')
              .otherwise('Senior'))
df.show()

#adding another column called Employe Status as if the experinec > 4 Full time, else Contract
print("Adding a column called Employe Status as if the experinec > 4 Full time, else Contract")
df = df.withColumn("Employe Status", when(df['Experience'] > 4, 'Full Time')
                   .otherwise('Contract'))

df.show()


#Deleting or a droping a column in a dataframe, we use drop("column name")
print("Droping or deleting a column named Employe status")
df = df.drop("Employe Status")

df.show()

# Renaming the Experience level column as Level using withColumnRenamed method
print("Renaming the Experience Level column as Level")
df = df.withColumnRenamed('Experience Level', 'Level')

df.show()

spark.stop()

