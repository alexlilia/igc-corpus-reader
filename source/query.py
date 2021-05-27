from elasticsearch import Elasticsearch

if __name__=="__main__":
    print("Initializing Elasticsearch...")
    es = Elasticsearch()

    query = "vera0sfg3en"
    print("Query",query)
    dsl = {"track_total_hits":True, "query": {"regexp": { "text": ".*%s.*" % query }}}
    
    res = es.search(dsl,index="bin")
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print(hit["_source"])
