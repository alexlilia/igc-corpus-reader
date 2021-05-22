import os
import xml, json
from zipfile import ZipFile
from elasticsearch import Elasticsearch
from tqdm import tqdm
from xml.etree import ElementTree
import xmltodict
from sys import stderr

SEP="0"

def get_entry(w):
    return "%s%s%s%s%s" % (w["#text"],SEP,w["@lemma"],SEP,w["@type"])

def get_words(s):
    sent = []
    for w in s:
        try:
            sent.append(get_entry(w))
        except:
            print("Skip entry %s" % w,file=stderr)
    return sent

def etree_to_dict(t):
    t = xmltodict.parse(ElementTree.tostring(t))
    t = t['ns0:TEI']['ns0:text']['ns0:body']
    sentences = []
    for div in t:
        for p in t[div]:
            for ss in t[div][p]:
                if type(ss) == type(""):
                    continue
                if type(ss['ns0:s']) == type([]):
                    for sss in ss['ns0:s']:
                        sentences.append(get_words(sss["ns0:w"]))
                else:
                    sentences.append(get_words(ss['ns0:s']["ns0:w"]))
    return [{"text":" ".join(s)} for s in sentences]

def build_elasticsearch(data_path):

    for fn in os.listdir(data_path):
        
        with ZipFile(os.path.join(data_path,fn),'r') as zip_file:
            filenames = zip_file.namelist()
            root = filenames[0]

        # get corpus id and what is the language used in this corpus, support English and French for now
            with zip_file.open(os.path.join(root, 'corpus.json'), 'r') as f:
                corpus_info = json.load(f)
                corpus_id = corpus_info['id']
                corpus_lang = 'icelandic'

            # get all documents' name(id)
            documents_ids = []
            for filename in filenames:
                if filename.endswith('.xml'):
                    documents_ids.append(filename.split('/')[-1][:-4])
        done_list = []
        todo_path = os.path.join('bin', fn + '_todo.json')
        done_path = os.path.join('bin', fn + '_done.json')
        # check if elasticsearch is set up or not. if not, create two json file
        if not os.path.exists('bin'):
            os.mkdir('bin')
        if not (os.path.exists(todo_path) and os.path.exists(done_path)):
            todo = json.dumps({'document_ids': documents_ids}, indent=4)
            done = json.dumps({'document_ids': done_list}, indent=4)
            with open(todo_path, 'w') as f:
                f.write(todo)
            with open(done_path, 'w') as f:
                f.write(done)
        # if exists, load json files and check if all documents have been processed
        with open(todo_path, 'r') as f:
            todo = json.load(f)
            documents_ids = todo['document_ids']
        with open(done_path, 'r') as f:
            done = json.load(f)
            done_list = done['document_ids']

    # for elastic search, setting up mapping
        mapping = {
            'settings': {
                'index': {
                    'number_of_shards': '1',
                    'max_result_window': '1000000',
                }
            },
            'mappings': {
                'properties': {
                    'text': {'type': 'text'},
                    },
                },
            }

        print("Initializing Elasticsearch...")
        es = Elasticsearch()

        if not es.indices.exists("bin"):
            es.indices.create(index="bin",
                body=mapping,
                timeout=60,
                ignore=[400, 404]
                )

        # iterate all documents and create an index for each document
        with ZipFile(os.path.join(data_path, fn), 'r') as zip_file:
            for i in tqdm(range(len(documents_ids))):
                doc_id = documents_ids[i]
                for root, dirs, files in os.walk("[^\.].xml$"):
                    paths = root.split(os.sep)
                    for path in paths:
                        doc_path = os.path.join(path, doc_id + '.xml')
                        with zip_file.open(doc_path, 'r') as f:
                            doc = ElementTree.fromstring(f.read())
                            doc = etree_to_dict(doc)
                            for i, s in enumerate(doc):
                                id = "%s@%u" % (doc_id,i)
                                if not es.exists(index="bin",id=id):
                                    es.create(index="bin",id=id, body=s)
                                    print("Created %s" % id)
                                    done_list.append(id)

        # If done, update the status of the done json file
        with open(done_path, 'w') as f:
            done = json.dumps({'document_ids': done_list}, indent=4)
            f.write(done)

        print("Done!")

        print(es.get(id="R-33-4941691@0",index="bin"))

        query = "hafa%ssþghen" % SEP
        print("Query",query)
        dsl = {"query": {"regexp": { "text": ".*%s.*" % query }}}

        res = es.search(dsl,index="bin")
        print("Got %d Hits:" % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            print(hit["_source"])


if __name__ == "__main__":
    
    DATA_PATH = '../data'
    
    build_elasticsearch(data_path=DATA_PATH)


