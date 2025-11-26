from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#creating a Spark Session
print("Creating a Spark Session...")
spark: SparkSession = (
    SparkSession.builder
    .appName("Handling_Data")
    .getOrCreate()
)

#Reading a CSV file into Dataframe
print("Reading a CSV file into Dataframe")
df = spark.read.csv("salaries.csv", header = True, inferSchema = True)
df.show()

#Droping Null Values
# this will drop all the rows where atleast one column has a null
# Since we are not dropping the nulls from original df we just removed null values from original df and 
# stored the filtered df into new dataframe called filtered_df
print("This is the result for using na.drop() with out any parameters:")
filtered_df = df.na.drop()
filtered_df.show() 

#This is with using parameter how
print("Dropping a row if any column have a null value:")
filtered_df = df.na.drop(how = 'any')
filtered_df.show()

print("Droping a row is all the columns have a null values:")
filtered_df = df.na.drop(how = 'all')
filtered_df.show()


#This is the example of using drop with parameters thresh
# (thresh = n where it removes rows if it doesnt have atleast n non nulls)
print("Droping the rows if it doesnt have atleast 2 non null values:")
filtered_df = df.na.drop(thresh = 2)
filtered_df.show()


#This is example of using drop with subset parameter
#subset is used when you want to remove rows based on specific column
print("Droping data where name is null:")
filtered_df = df.na.drop(subset = ["Name"])
filtered_df.show()


#filling missing values or nulls
#lets suppose fill all nulls in the dataframe with value as No Data
#if you pass string as input it replace all string columns with input
#if you pass int as input it replace all int columns with input
print("Filling all Nulls with value 'No Data':")
filtered_df = df.na.fill('No Data')
filtered_df.show()

print("Filling all Nulls with value 0:")
filtered_df = df.na.fill(0)
filtered_df.show()


print("Filling Null values based on Columns:")
print("Name : Unknown \nAge : 100 \nExperience : 0 \nSalary : 0")
filtered_df = df.na.fill({'Name':'UnKnown','Age':100, 'Experience':0,'Salary':0})
filtered_df.show()

#I want to fill null values with mean
# I can get mean form describe with out calculating
print("Printing describe from df:")
described_df = df.describe()
described_df.show()

mean_row = described_df.filter(F.col('summary')=='mean').collect()[0]

print("Filling Null values with mean on Columns:")
filtered_df = df.na.fill({'Name':'UnKnown','Age':mean_row['Age'], 'Experience':mean_row['Experience'],'Salary':mean_row['Salary']})
filtered_df.show()

spark.stop()