import os
from pathlib import Path
import glob
import xml, json
from zipfile import ZipFile
from elasticsearch import Elasticsearch
from tqdm import tqdm
from xml.etree import ElementTree
import xmltodict
from sys import stderr
import argparse

SEP="0"

log_file = open("log.txt","w")

def etree_to_dicts(t):
    # Get list of div elements in the document
    divs = t[1][0]

    sents = []
    for div in divs:
        assert("{http://www.tei-c.org/ns/1.0}div" in div.tag)
        for p in div:
            assert("{http://www.tei-c.org/ns/1.0}p" in p.tag)
            for s in p:
                assert("{http://www.tei-c.org/ns/1.0}s" in s.tag)
                sent = []
                for w in s:
                    assert("{http://www.tei-c.org/ns/1.0}w" in w.tag or 
                           "{http://www.tei-c.org/ns/1.0}c" in w.tag)
                    attrib = w.attrib
                    try:
                        sent.append("%s%s%s%s%s" % (w.text,
                                                    SEP,
                                                    w.attrib["lemma"], 
                                                    SEP,
                                                    w.attrib["type"]))
                    except:
                        sent.append("%s%s%s%s%s" % (w.text,SEP,w.text,SEP,w.text))
                sents.append({"text": " ".join(sent)})
    return sents

def build_elasticsearch(zip_fn,corpus_json):

    with ZipFile(zip_fn,'r') as zip_file:
        filenames = zip_file.namelist()

        with open(corpus_json, 'r') as f:
            corpus_info = json.load(f)
            corpus_id = corpus_info['id']
            corpus_lang = 'icelandic'

        # get all documents' name(id)
        documents_ids = []
        for filename in filenames:
            if filename.endswith('.xml'):
                documents_ids.append(filename.split('/')[-1][:-4])
        
    done_list = []
    zip_fn_prefix = os.path.split(zip_fn)[1]
    todo_path = os.path.join('bin', zip_fn_prefix + '_todo.json')
    done_path = os.path.join('bin', zip_fn_prefix + '_done.json')

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

    print("Initializing Elasticsearch...",file=log_file)
    es = Elasticsearch()

    if not es.indices.exists("bin"):
        print("Creating index")
        es.indices.create(index="bin",
                          body=mapping,
                          timeout=60,
                          ignore=[400, 404]
                          )

    # iterate all documents and create an index for each document
    with ZipFile(zip_fn, 'r') as zip_file:
        results = []
        for i, doc_path in zip(tqdm(range(len(documents_ids))), zip_file.namelist()):
            doc_id = documents_ids[i]

            if (not doc_path.startswith("_")) and doc_path.endswith("xml"):
                name = doc_path
        
                with zip_file.open(name, 'r') as f:
                    doc = ElementTree.fromstring(f.read())
                    doc = etree_to_dicts(doc)
                    print("SENTENCES:",len(doc),file=log_file)
                    for i, s in enumerate(doc):
                        id = "%s@%u" % (doc_id,i)
                        if not es.exists(index="bin",id=id):
                            es.create(index="bin",id=id, body=s)
                            print("Created %s" % id,file=log_file)
                            done_list.append(id)

    # If done, update the status of the done json file
    with open(done_path, 'w') as f:
        done = json.dumps({'document_ids': done_list}, indent=4)
        f.write(done)

        while results != []:
            if results[-1].ready():
                results.pop(-1)
                print(len(results))
            
            
    print("Done!",file=log_file)

parser = argparse.ArgumentParser()
parser.add_argument("--corpus_json", type=str)
parser.add_argument("--zip_file", type=str)
args = parser.parse_args()

if __name__ == "__main__":
    build_elasticsearch(zip_fn=args.zip_file,
                        corpus_json=args.corpus_json)
