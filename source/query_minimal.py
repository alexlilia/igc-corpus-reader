import cProfile
from re import RegexFlag
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from sys import argv
import csv
import regex
import datetime

MAX_RES=15

def main(argv):
    print("Initializing Elasticsearch...")
    es = Elasticsearch()

    query = argv[1]
    max_matches = int(argv[2])

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
        scroll='1m',
        size=10000)
    
    print("Got %d hits" % res['hits']['total']['value'])
    print("Displaying maximally %u results:" % MAX_RES)
    for i, hit in enumerate(res['hits']['hits']):
        j = regex.search('[\w\S]*%s[\w\S]*' % query, hit["_source"]["text"], 
                         flags=regex.UNICODE|regex.IGNORECASE)
        print("%u:\t%s:\t%s" % (i+1,j,hit["_source"]["text"]))



    def create_csv_format():
        my_dict = {}

        for entry in all_results:
            my_dict['source'] = entry['_source']['text']
            
            yield my_dict

    date = datetime.datetime.now().strftime("%Y%m%d_%I%M%S%p")

    with open(f"{query}_{date}.csv","w",encoding="UTF-8") as f:
        total_matches = 0
        header_present = False
        
        for dict in create_csv_format():
            total_matches += 1
            print(dict["source"],file=f)

            if total_matches >= max_matches:
                return

if __name__=="__main__":
#    cProfile.run("main(argv)")
    main(argv)
