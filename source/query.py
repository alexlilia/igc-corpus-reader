from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from sys import argv
import csv
import re

MAX_RES=15

if __name__=="__main__":
    print("Initializing Elasticsearch...")
    es = Elasticsearch()

    query = argv[1]
    print("Query:",query)

    # dsl = {"track_total_hits":True, "size": MAX_RES, "query": {"regexp": { "text": ".*%s.*" % query }}}
    dsl = {"track_total_hits":True, 
        "size": MAX_RES, 
        "query": { 
            "regexp": { 
                "text": ".*%s.*" % query }
                },
        }
    res = es.search(dsl,
        index="bin",
        scroll='1h')

    my_dict = dict()

    print("Got %d hits" % res['hits']['total']['value'])
    print("Displaying maximally %u results:" % MAX_RES)
    for i, hit in enumerate(res['hits']['hits']):
        j = re.search(f"\w+{query}\w+",hit["_source"]["text"]).group(0)
        print("%u:\t%s:\t%s" % (i+1,j,hit["_source"]["text"]))

    my_dict = {}
    with open(f"{query}.csv","w",encoding="UTF-8") as f:
        header_present  = False
        for hit in res['hits']['hits']:
            j = re.search(f"\w+{query}\w+",hit["_source"]["text"]).group(0).split("0")
            my_dict['source'] = hit['_id']
            my_dict['token'] = j[0]
            my_dict['lemma'] = j[1]
            my_dict['tag'] = j[2]
            
            print(my_dict)
            if not header_present:
                w = csv.DictWriter(f, my_dict.keys())
                w.writeheader()
                header_present = True

            w.writerow(my_dict)