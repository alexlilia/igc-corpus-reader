import os
import re
import json
import time
import random
from zipfile import ZipFile
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from tqdm import tqdm


class CorpusReader():
    """
    Take a French or English corpus and then build an Index by using Elasticsearch
    "https://www.elastic.co/"

    Retrieve instances from the index fast
    """

    def __init__(self, DATA_PATH, ZIPFILE_NAME, NUM_OF_SAMPLES=500):
        """
        Initial function which get the information of this corpus and set up an Elasticsearch instance.

        Arguments:
        ----------
        DATA_PATH: str, the path of datasets
        ZIPFILE_NAME: str, the name of dataset which should be a zipfile
        NUM_OF_SAMPLES: int, the number of instances will be retrieved, default=500

        """
        self.data_path = DATA_PATH
        self.zipfile_name = ZIPFILE_NAME
        self.num_of_samples = NUM_OF_SAMPLES
        self.root = ''
        self.corpus_id = ''
        self.corpus_lang = ''
        self.documents_meta_id = ''
        self.annotations_id = ''
        self.documents_ids = []
        self.es = Elasticsearch()

        with ZipFile(os.path.join(self.data_path, self.zipfile_name), 'r') as zip_file:
            filenames = zip_file.namelist()
            self.root = filenames[0]

            # get corpus id and what is the language used in this corpus, support English and French for now
            with zip_file.open(os.path.join(self.root, 'corpus.json'), 'r') as f:
                corpus_info = json.load(f)
                corpus_id = corpus_info['id']
                if corpus_info['languages'][0] == 'fr-FR':
                    self.corpus_lang = 'french'
                else:
                    self.corpus_lang = 'english'

            # get annotation files information
            with zip_file.open(os.path.join(self.root, 'CorpusStructure.json'), 'r') as f:
                groups_info = json.load(f)
                for bucket in groups_info['buckets']:
                    if bucket['name'] == 'Transcode task bucket':
                        documents_meta_id = bucket['id']
                    if bucket['name'] == 'Annotations':
                        annotations_id = bucket['id']

        # get all documents' name(id)
        documents_ids = []
        for filename in filenames:
            if filename.startswith(os.path.join(self.root, 'document')) and filename.endswith('.json'):
                documents_ids.append(filename.split('/')[-1][:-5])

        self.corpus_id = corpus_id
        self.documents_meta_id = documents_meta_id
        self.annotations_id = annotations_id
        self.documents_ids = documents_ids
        self.documents_amount = len(documents_ids)


    def doc_filter(self, target):
        """
        By using already built index, filter out documents that contain a target word

        Arguments:
        ----------
        target: str, input word

        Returns:
        ----------
        list, a list of document names(ids)

        """

        dsl = {
            "_source": ['id'],
            "query": {
                "match": {"text": target}
            },
        }

        filtered_doc_ids = []
        results = scan(self.es, query=dsl)
        for r in results:
            filtered_doc_ids.append(r['_source']['id'])

        # randomly pick N documents if it exceeds NUM_OF_SAMPLES
        if self.num_of_samples != -1 and len(filtered_doc_ids) > self.num_of_samples:
            # randomly sample N instances from them
            filtered_doc_ids = random.sample(filtered_doc_ids, self.num_of_samples)

        return filtered_doc_ids

    def search(self, target, window_size=20, pos=".*"):
        """
        Searching function

        Arguments:
        ----------
        target: str, input word
        window_size: int, the number of left/right context tokens, default=20
        pos: str, specific pos tag of a target word, default='.*'

        Returns:
        ---------
        list: a list of triples [([LEFT CONTEXT TOKENS], target word, [RIGHT CONTECT TOKENS]), ...]
        """

        doc_ids = self.doc_filter(target)
        instances = []
        with ZipFile(os.path.join(self.data_path, self.zipfile_name), 'r') as zip_file:
            for doc_id in tqdm(doc_ids):
                doc_path = os.path.join(self.root, 'groups', self.annotations_id, doc_id + '.json')
                try:
                    with zip_file.open(doc_path, 'r') as f:
                        annotation = json.load(f)
                        tokens = annotation[self.corpus_id][self.annotations_id]['TOKEN']
                        for offset, token in enumerate(tokens):
                            if token['string'] == target and re.match(pos, token['category']):
                                left_start = max(0, offset - window_size)
                                right_end = min(len(tokens), offset + window_size + 1)
                                left_context = [token['string'] for token in tokens[left_start:offset]]
                                target = tokens[offset]['string']
                                right_context = [token['string'] for token in tokens[offset + 1:right_end]]
                                instances.append((left_context, target, right_context))
                except:
                    continue
        if self.num_of_samples != -1 and len(instances) > self.num_of_samples:
            return random.sample(instances, self.num_of_samples)
        return instances


if __name__ == "__main__":
    # Datasets Names: assnat_slim, covid_slim, assnat, covid
    DATA_PATH = '../incoming'
    FOLDER_NAME = 'assnat_slim.zip'

    reader = CorpusReader(DATA_PATH, FOLDER_NAME, NUM_OF_SAMPLES=200)
    instances = reader.search("and")

    for i in instances:
        print(i)