from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#Dispaling the data
def data():
    try:
        res = es.search(index="emp11",
                       body={"from": 0, "size": 1000, "query": {"match_all": {}}})
        print(res)
    except:
        print("Syntax error")




# Dispalying the employees who belongs to the regions midwest and south and who has hike greater than 14
def region():
    try:
        res = es.search(index="metafile", body={
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"Region.keyword": ["Midwest", "South"]}}
                    ],
                    "should": [
                        {"range": {"Hike.keyword": {"gte": 14}}}
                    ]
                }
            }
        })
        print(res)
    except SyntaxError:
        pass


# Count number of employees in each Region
def cnt_emp_reg():
    try:
        # Employees in midwest
        midwest = es.count(index='metafile', body={'query': {'match': {'Region': 'Midwest'}}})
        print(midwest)
        # Employees in south region
        south = es.count(index='metafile', body={'query': {'match': {'Region': 'South'}}})
        print(south)
        # Employees in west region
        west = es.count(index='metafile', body={'query': {'match': {'Region': 'West'}}})
        print(west)
        # Employees in  region Norteast
        northeast = es.count(index='metafile', body={'query': {'match': {'Region': 'Northeast'}}})
        print(northeast)
    except SyntaxError:
        print("Records not found")
        pass


def gen_sal():
    try:
        # Dispalying the employess based on Gender here printed Male employees details in desending order
        print(es.search(index="metafile",
                    body={"query": {"match": {"Gender": "M"}}, "sort": {"Salary.keyword": {"order": "desc"}}}))

        # Dispalying the employess based on Gender here printed Female employees details in desending order
        print(es.search(index="metafile",
                    body={"query": {"match": {"Gender": "F"}}, "sort": {"Salary.keyword": {"order": "desc"}}}))
    except SyntaxError:
        print("Documents not found")


# Sorting the Employees based on salary
def srt_emp():
    try:
        result = es.search(index="metafile", body={"sort": {"Salary.keyword": {"order": "desc"}}})
        print(result)
    except:
        print("Records not found")


# #Dispalying employees joining count based on different months
def cnt_emp_mnt():
    try:
        print(es.search(index="metafile",
                        body={"size": 0, "aggs": {"by_month": {"terms": {"field": "Short Month.keyword"}}}}))
    except:
        print("records not found")


#Dispalying the employees who belong to midwest and joined in jan month
def exp_quries():
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"Region.keyword": "Midwest"}
                    },
                    {
                        "bool": {
                            "should": [
                                {"term": {"Short Month.keyword": "Jan"}}
                            ]
                        }
                    }
                ]
            }
        }
    }


    res = es.search(
      index = 'metafile',
      size = 100,
      body = query)
    print(res)


