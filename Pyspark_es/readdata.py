from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession \
    .builder \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.csv("/home/phani/Elasticsearch_spark/employee.csv", header=True)
df = df.fillna(0)
print(df.show())

print(df.printSchema())


df1 = df.select('Emp ID', 'First Name', 'Middle Initial', 'Gender', 'E Mail', 'Short Month', 'Salary', 'County', 'City', 'State', 'Region', regexp_replace(col("Last % Hike"), "%", "").alias("Hike"))
df1.show()
#

#
df1 = df1.withColumn("Salary", df1["Salary"].cast('int'))
df1 = df1.withColumn("Hike", df1["Hike"].cast('int'))
#
print(df1.printSchema())
#
df1.write.csv('mydata')




