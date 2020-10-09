from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# set environment variable PYSPARK_SUBMIT_ARGS
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/phani/Desktop/elasticsearch-hadoop-7.9.2/dist/elasticsearch-hadoop-7.9.2.jar pyspark-shell'

spark = SparkSession \
    .builder \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.csv("employee.csv", header=True)
print(df.show())

try:
    df = df.fillna(0)
    print(df.show())
except SyntaxError:
    pass


df1 =  df.select('Emp ID', 'First Name', 'Middle Initial', 'Gender', 'E Mail', 'Salary', 'County', 'City', 'State', 'Region', regexp_replace(col("Last % Hike"), "%", "").alias("Hike"))
print(df1.show())


print(df1.printSchema())

df1 = df1.withColumn("Salary",df1["Salary"].cast('int'))
df1 = df1.withColumn("Hike",df1["Hike"].cast('int'))

print(df1.printSchema())

## Count number of employees in each Region, country, city

try:
    #Showing the different regions
    print(df1.select("Region").distinct().show())
    #number of employees based on Region
    print(df1.groupBy('Region').count().show())
    #Number of employees based on Country
    print(df1.groupBy('County').count().show())
    #Number of employees based on city
    print(df1.groupBy('City').count().show())
except SyntaxError:
    print("Records not found")
    pass

# orderby gender and salary

gender = df1.orderBy(df1.Gender)
print(gender.show())


try:
    print("Total no of male employees")
    print(gender.filter(gender.Gender == 'M').count())
except SyntaxError:
    pass
try:
    print("Total no of Female employees")
    print(gender.filter(gender.Gender == 'F').count())
except SyntaxError:
    pass

# Order by salary and gender
print(df1.orderBy(df1.Gender, df1.Salary).show())

# Salary wise sort employee data
print(df1.sort("Salary").select('Emp ID', 'Salary', 'County').show())

try:
    print("Average salary of a employee is ")
    print(df1.agg({'Salary': 'avg'}).show())
except:
    print("Records not found")

print("Maximum salary of a employee is ")
print(df1.agg({'Salary': 'max'}).show())

print("Minimum salary of a employee is ")
print(df1.agg({'Salary': 'min'}).show())

# Employee Summary table

summary = df1.select('Emp ID', 'Salary', 'Hike', 'Region')
print(summary.show())
# printing the summary
print(summary.describe().show())

print("Total employees whose salary is greater than average salary")
print(summary.filter(summary['Salary'] > 119992).count())

print("Total employees whose got hike above average value")
print(summary.filter(summary['Hike'] > 14.9987616).count())

print("Total employees whose salary is below average salary")
print(summary.filter(summary['Salary'] < 119992).count())

print("Total employees whose got hike below average value")
print(summary.filter(summary['Hike']<14.9987616).count())

try:
    # Employess from different regions
    Midwest = summary.filter(summary['Region'] == 'Midwest').count()
    print("Total Employees from Midwest: ", Midwest)

    South = summary.filter(summary['Region'] == 'South').count()
    print("Total Employees from South: ", South)

    West = summary.filter(summary['Region'] == 'West').count()
    print("Total Employees from West: ", West)

    Northeast = summary.filter(summary['Region'] == 'Northeast').count()
    print("Total Employees from Northeast: ", Northeast)
except SyntaxError:
    pass







