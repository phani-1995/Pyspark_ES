
import findspark
from elasticsearch import Elasticsearch
import requests
from pprint import pprint

findspark.init("/home/phani/spark/spark-2.4.5-bin-hadoop2.7")
from pyspark.sql import SparkSession, functions as func

res = requests.get('http://localhost:9200')
pprint(res.content)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

spark = SparkSession.builder.appName('task').getOrCreate()

df = spark.read.format("csv").option("header","true").load("/home/phani/datasets/spacemissions.csv")
df.show(5)


print(f"Total records present: {df.count()}")



df.write.format(
    "org.elasticsearch.spark.sql"
).option(
    "es.resource", '%s' % ('missions_space')
).option(
    "es.nodes", 'localhost'
).option(
    "es.port", '9200'
).save()
print("Successfully ingested data into Elasticsearch!")

es.indices.put_settings(index="missions_space", body = {"index" : { "max_result_window" : 5000000 }})

es.count(index= "missions_space")
