# Large XML Corpus Extractor
This repository is based on [A Dissimilarity-based Concordancer](https://github.com/shuanggu-gs/a-dissimilarity-based-concordancer) and provides a tool which can be used to index and query a large XML-based text corpus using Elasticsearch. The results are then writable to a text file. This particular project was developed for the Icelandic Gigaword corpus [(IGC; Steingrimsson et al. 2018)](http://igc.arnastofnun.is) and contains sample data from this corpus. 

---
## Table of contents
- [Large XML Corpus Extractor](#large-xml-corpus-extractor)
  - [Table of contents](#table-of-contents)
  - [Description](#description)
    - [source](#source)
    - [data](#data)
  - [Setting up Elasticsearch](#setting-up-elasticsearch)
  - [Running the Extractor](#running-the-extractor)
    - [Step 1: Index the Corpus](#step-1-index-the-corpus)
      - [Only one zip file](#only-one-zip-file)
      - [Multiple zip files](#multiple-zip-files)
      - [Reindexing the corpus (important!)](#reindexing-the-corpus-important)
    - [Step 2: Query the corpus](#step-2-query-the-corpus)
      - [Fast query (`query_minimal.py`)](#fast-query-query_minimalpy)
      - [Long query (`query.py`)](#long-query-querypy)

---

## Description
There are two modules included in this package: **source** and **data**. 

### source
*   `initialize_index.py` : constructs an index from the zip file(s) in **data** and is specified for maximum sentence length
*   `query.py` : queries the indexed corpus using a regex match and writes all results to a text output (very slow, but recommended for very big queries)
*   `query_minimal.py` : similar functionality to `query.py` but returns results faster and can be specified for different output sizes (can also output all results, but not recommended for very big queries)

### data 
*   `Gigaword_small.zip` : a single XML file in a nested directory 
*   `Gigaword_large.zip` : multiple XML files in the same nested directory structure
*   `corpus.json` : metadata that you supply about your corpus

---

## Setting up Elasticsearch
1.  Install [Elasticsearch](https://www.elastic.co/downloads/elasticsearch)
2.  Get Elasticsearch up and running.
    *   You can navigate to its installation directory `cd YOUR_PATH/elasticsearch-7.13.2` and then run its binary `bin/elasticsearch-7.13.2`
    *   Or, if installed using Homebrew on macOS, then you can start it as a service `brew services start elastic/tap/elasticsearch-full`
3.  Check `curl http://localhost:9200/` to make sure that Elasticsearch has started

---

## Running the Extractor
### Step 1: Index the Corpus
In this step, we use XML header tags to ensure that every word in the corpus is a searchable string of the format `token0lemma0tag`. The separator is always `0`. Sentence structures are preserved, and so when queried, every match will be returned embedded in its original sentence.

Navigate to the **source** directory `cd YOUR_PATH/igc-corpus-reader/source` and run the indexer, which takes 3 arguments (the corpus zip file `--zip_file`, the corpus metadata file `--corpus_json`, and the maximum sentence length `--max_sent_len`):

#### Only one zip file
```
python3 initialize_index.py --zip_file="Gigaword_small.zip" --corpus_json="corpus.json" --max_sent_len=1000
```

#### Multiple zip files

If you have multiple zip files in **data** that you would like to index, then you can create a list of all the zip files you'd like to index (e.g., `Gigaword_list.txt`) and use a simple for-loop:

```
for f in `cat Gigaword_list.txt`; do python3 initialize_index.py --zip_file=$f --corpus_json='corpus.json --max_sent_len=1000'; done
```

---

 #### Reindexing the corpus (important!)

The indexed corpus will be saved to `igc-corpus-reader/bin` and so this indexation step only needs to be run once. If you want or need to re-index the corpus, remember to delete all the files in `bin` and run `curl -XDELETE http://localhost:9200/bin` or else the new index will just append to the old one and you could end up with duplicates.

---

### Step 2: Query the corpus
#### Fast query (`query_minimal.py`)
 This takes 2 arguments (the regex query itself and the maximum number of results to write to a csv):
```
python3 query_minimal.py "hola0nvfe" 10000
```
This will look for any string matches in the corpus with the sequence `hola0nvfe`, and `10000` indicates that 10,000 results will be printed to an output csv. If you want to print all results to the output csv, you can enter `-1`.

The results in the output csv will be printed as one sentence (each containing *at least one* match) per row. 

---

#### Long query (`query.py`)
Navigate to the **source** directory `cd YOUR_PATH/igc-corpus-reader/source` and run the query, which takes 1 argument (the regex query):

```
python3 query.py "hola0nvfe"
```
This will look for any string matches in the corpus with the sequence `hola0nvfe`...

```
1:	<regex.Match object; span=(9, 23), match='Hola0hola0nvfe'>:	,,0,,0ta Hola0hola0nvfe Espania0Espania0nven-s !0!0!
2:	<regex.Match object; span=(42, 57), match='holna0hola0nvfe'>:	Á0á0af einum0einn0tfkeþ átján0átján0tfvfe holna0hola0nvfe golfhring0golfhring0nven gengur0ganga0sfg3en maður0maður0nken níu0níu0tfkfþ til0til0aa tíu0tíu0tfkfo kílómetra0kílómetri0nkfe .0.0.
3:	<regex.Match object; span=(88, 117), match='vinnsluholna0vinnsluhola0nvfe'>:	Það0það0fphen fól0fela0sfg3eþ í0í0af sér0sig0fpheþ borun0borun0nveo tveggja0tveir0tfvfe vinnsluholna0vinnsluhola0nvfe og0og0c varmaskiptastöðvar0varmaskiptastöð0nvee .0.0.
```
...and will then print a 4-column output csv (and not a sentence, as in `query_minimal.py`):

| source                                | token     | lemma     | tag   |
| ---                                   | ---       | ---       | ---   |
| data_MIM_morgunbladid.zip.40579752	| borhola   | borhola   | nvfe  |
| data_MIM_morgunbladid.zip.40635343	| borholna  | borhola   | nvfe  |
| data_MIM_morgunbladid.zip.40686869	| holna     | hola	    | nvfe  |

---