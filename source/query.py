from re import RegexFlag
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from sys import argv
import csv
import regex
import datetime

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
                "text": '.*%s.*' % query }
                }
        }

    res = es.search(dsl,
        index="bin",
        request_timeout=30)
    
    all_results = helpers.scan(client=es,
        query=dsl,
        index="bin",
        scroll='1h')
    
    print("Got %d hits" % res['hits']['total']['value'])
    print("Displaying maximally %u results:" % MAX_RES)
    for i, hit in enumerate(res['hits']['hits']):
        j = regex.search('[\w\S]*%s[\w\S]*' % query, hit["_source"]["text"], flags=regex.UNICODE)
        print("%u:\t%s:\t%s" % (i+1,j,hit["_source"]["text"]))

    my_dict = dict()
    date = datetime.datetime.now().strftime("%Y%m%d_%I%M%S%p")

    with open(f"{query}_{date}.csv","w",encoding="UTF-8") as f:
        header_present  = False 
        for entry in all_results:
            # print(entry)
            j = regex.search('[\w\S]*%s[\w\S]*' % query, entry["_source"]["text"], flags=regex.UNICODE).group(0).split("0")
            # print(j)
            my_dict['source'] = entry['_id']
            my_dict['token'] = j[0]
            my_dict['lemma'] = j[1]
            my_dict['tag'] = j[2]
            # print(regex.search('\w*%s\w*' % query, entry["_source"]["text"], flags=regex.UNICODE))

            # print(my_dict)
            if not header_present:
                w = csv.DictWriter(f, my_dict.keys())
                w.writeheader()
                header_present = True

            w.writerow(my_dict)