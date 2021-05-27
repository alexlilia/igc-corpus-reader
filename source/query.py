from elasticsearch import Elasticsearch
from sys import argv

MAX_RES=15

if __name__=="__main__":
    print("Initializing Elasticsearch...")
    es = Elasticsearch()

    query = argv[1]
    print("Query:",query)

    dsl = {"track_total_hits":True, "size": MAX_RES, "query": {"regexp": { "text": ".*%s.*" % query }}}    
    res = es.search(dsl,index="bin")

    print("Got %d hits" % res['hits']['total']['value'])
    print("Displaying maximally %u results:" % MAX_RES)
    for i, hit in enumerate(res['hits']['hits']):
        print("%u:\t%s" % (i+1,hit["_source"]["text"]))
