from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .getOrCreate()
sc = spark.sparkContext


def read_data():
    df = spark.read.csv("/home/phani/Elasticsearch_spark/Elastic_search_practise/1000-Records/employeedata.csv",
                        header=True)
    df = df.fillna(0)
    df = df.withColumn("Salary", df["Salary"].cast('int'))
    df1 = df.select('Emp ID', 'First Name', 'Middle Initial', 'Gender', 'E Mail', 'Short Month', 'Salary', 'County',
                    'City', 'State', 'Region')
    return df1


