import os
import json
import time
from random import sample, seed
from initialize_index import build_elasticsearch
from corpus_reader import CorpusReader
# from cluster import *


seed(3)

with open('./configuration.json', 'r') as f:
    parameters = json.load(f)

# Datasets Names: assnat_slim, covid_slim, assnat, covid
DATA_PATH = parameters['DATA_PATH']
FOLDER_NAME1 = parameters['FOLDER_NAME1']
FOLDER_NAME2 = parameters['FOLDER_NAME2']

# CLUSTER_METHOD = parameters['CLUSTER_METHOD'] # {kmeans, dbscan, optics}
LANG_ID = parameters['LANG_ID'] # {'en', 'fr'}; we only have icelandic
FEATURE_TYPE = parameters['FEATURE_TYPE']  # {sbert, word2vec, fasttext}
REDUCTION_TYPE = parameters['REDUCTION_TYPE']  # {'pca', 'tsne', None}
N_COMPONENTS = parameters['N_COMPONENTS']

MODEL_PATH = parameters['MODEL_PATH']

NUM_OF_SAMPLES = parameters['NUM_OF_SAMPLES']
WINDOW_SIZE = parameters['WINDOW_SIZE']

TEST_FOLDER = parameters['TEST_FOLDER']
TEST_FILE = parameters['TEST_FILE']

MODE = parameters['MODE']


if __name__ == '__main__':

    reader = CorpusReader(DATA_PATH, FOLDER_NAME, NUM_OF_SAMPLES=NUM_OF_SAMPLES)

    todo_path = os.path.join('bin', FOLDER_NAME + '_todo.json')
    done_path = os.path.join('bin', FOLDER_NAME + '_done.json')

    if os.path.exists(todo_path) and os.path.exists(done_path):
        with open(todo_path, 'r') as todo_f:
            todo = json.load(todo_f)
            todo_list = todo['document_ids']
        with open(done_path, 'r') as done_f:
            done = json.load(done_f)
            done_list = done['document_ids']

        assert reader.documents_amount == len(todo_list), "Something wrong within the corpus, please delete 'bin' folder and re-run it."
        if len(todo_list) != len(done_list):
            build_elasticsearch(data_path=DATA_PATH, zipfile_name=FOLDER_NAME)
    else:
        build_elasticsearch(data_path=DATA_PATH, zipfile_name=FOLDER_NAME)

    # if MODE == 'debug':
    #     debug = True # if debug=true, will save plots of cluster results
    #     word_list = ['infection', 'paucity', 'lysosome', 'compensatory', 'propagate', 'respiratory', 'immune', 'antibody']

    #     # more test words can be get from here
    #     # with open(os.path.join(TEST_FOLDER, TEST_FILE), 'r') as f:
    #     #     debug = False
    #     #     test_json = json.load(f)
    #     #     word_list = sample(test_json['tokens'], 50)

    #     reports = []
    #     start = time.time()
    #     for word in word_list:
    #         # word = input("Input a target word: ")
    #         instances = reader.search(word, window_size=WINDOW_SIZE)
    #         if len(instances) == 0:
    #             print(word)
    #             print("No instance found, please change the target word.")
    #             continue

    #         if REDUCTION_TYPE and len(instances) < N_COMPONENTS:
    #             print("Number of instances is less than N_COMPONENTS, please change the target word.")
    #             continue

    #         results, random_scores, cluster_scores = concordancer(instances,
    #                                                               cluster_method=CLUSTER_METHOD,
    #                                                               feature_type=FEATURE_TYPE,
    #                                                               lang_id=LANG_ID,
    #                                                               reduction_type=REDUCTION_TYPE,
    #                                                               n_components=N_COMPONENTS,
    #                                                               model_path=MODEL_PATH,
    #                                                               debug=debug)

    #         for left_context, target, right_context in results:
    #             print(' '.join(left_context) + ' ' + target.upper() + ' ' + ' '.join(right_context))
    #         if len(results) < 2:
    #             continue

    #         report = {
    #             word: {
    #                 'results': results,
    #                 'random_scores': random_scores,
    #                 'cluster_scores': cluster_scores,
    #             }
    #         }
    #         reports.append(report)

        print('Processing time:', (time.time() - start) / 60)
        if not os.path.exists('../results'):
            os.mkdir('../results')
        with open(os.path.join('../results', '_' + TEST_FILE + '_results.json'), 'w') as f:
            json.dump(reports, f)

    if MODE == 'user':
        debug = False
        while (True):
            word = input("Input a target word: ")
            instances = reader.search(word, window_size=WINDOW_SIZE)
            if len(instances) == 0:
                print(word)
                print("No instance found, please change the target word.")
                continue

            if REDUCTION_TYPE and len(instances) < N_COMPONENTS:
                print("Number of instances is less than N_COMPONENTS, please change the target word.")
                continue

            results, random_scores, cluster_scores = concordancer(instances,
                                                                  cluster_method=CLUSTER_METHOD,
                                                                  feature_type=FEATURE_TYPE,
                                                                  lang_id=LANG_ID,
                                                                  reduction_type=REDUCTION_TYPE,
                                                                  n_components=N_COMPONENTS,
                                                                  model_path=MODEL_PATH,
                                                                  debug=debug)

            for left_context, target, right_context in results:
                print(' '.join(left_context) + ' ' + target.upper() + ' ' + ' '.join(right_context))
            if len(results) < 2:
                continue


