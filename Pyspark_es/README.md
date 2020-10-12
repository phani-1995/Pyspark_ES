## Employee Data cleaning using pyspark and extracting the results from that using pyspark and elastic search

```
I have Downloaded the dataset which consists of 5 million records Employee data.
I have cleaned the data and saved the data using pyspark.
I have solved the following questions using pyspark.
```
#### Questions
'''
1. Count number of employees in each Region, Country and City. 
2. Generate employee Sumary.
3. Orderby Gender and Salary.
4. Summarise the number of emp joined and hickes granted based on month. 
5. Salary wise sort employee data.
'''

### Resource  
> Employee Data:   http://eforexcel.com/wp/downloads-16-sample-csv-files-data-sets-for-testing/

## Elastic search 

'''
1.I have downloaded Elastic search and logstash and i have installed elastic search 
  To check if elastic search is runnig or not check it in localhost:9200.
2.open the logstash config file and created the logstashexm.conf file and declear the configuration.
  We are using logstash file to ingest the data to elastic search
  We can also ingest data using elasticsearch loader.
3.Run the logstash config file after running itb successfully check it in localhost with index name.
4.After ingesting the data i have solved the solutions for the above questions
'''
### Resuorce
> https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
