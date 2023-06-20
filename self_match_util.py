from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import time
import re

class SelfMatchUtil:
    def __init__(self, index_name, data):
        self.index_name = index_name
        self.data = data

        # create es client
        password='FtxkVZYOjZBdsLqbryvc'
        ca_certs_dir='/home/vinhvan/elasticsearch-8.4.2/config/certs/http_ca.crt'
        self.es = Elasticsearch(
                    "https://localhost:9200",
                    ca_certs=ca_certs_dir,
                    basic_auth=("elastic", password),
                    request_timeout=60
                )
        print(self.es.info())

        self.self_index()

    def self_index(self):
        index_name = self.index_name
        data = self.data

        # delete index if exist
        # if self.es.indices.exists(index_name):
        #     self.es.indices.delete(index_name)

        self.es.options(ignore_status=[400,404]).indices.delete(index=index_name)

        # create mapping
        setting = {
                    "similarity": {
                        "scripted_tfidf": {
                            "type": "scripted",
                            "script": {
                                # "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
                                # this tf idf score remove idf
                                "source": "double tf = Math.sqrt(doc.freq); double idf =  1.0; double norm = 1/Math.sqrt(doc.length + 1); return query.boost * tf * idf * norm;"
                            }
                        },
                        "custom_bm25": {
                            "type": "BM25",
                            "b": 0.65,
                            "k1": 1.2
                        }

                    },
                    "analysis": {
                        "analyzer": {
                            "my_standard_shingle_no_unique": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "my_shingle_filter"]
                            }
                        },
                        "filter": {
                            "my_shingle_filter": {
                                "type": "shingle",
                                "min_shingle_size": 2,
                                "max_shingle_size": 3,
                                "output_unigrams": True
                            }
                        }
                    }
                }

        mapping = {
            "properties": {
                "full_name_no_unique": {
                    "type": "text",
                    "index_phrases": True,
                    "store": True,
                    "analyzer": "my_standard_shingle_no_unique",
                    # "similarity": "custom_bm25"
                }
            }
        }

        self.es.indices.create(index=index_name, mappings=mapping, settings=setting)

        # index into es db

        for i in range(0, len(data), 1000):
            # print(i)
            # print(len(raw_dict_terms))
            # print(data[i:i+1000])
            actions = [
                {
                    "_index": index_name,
                    "_source": {
                        "full_name_no_unique": i
                    }
                }
                for i in data[i:i+1000]
            ]
            helpers.bulk(self.es, actions)

        print("DATA SIZE: ", len(data))
        time.sleep(2)
        resp = self.es.count(index=index_name, query={"match_all": {}})
        print(resp) 


    @staticmethod
    def get_match_term_query(raw_term, rank_list_size=10):
        j = {
            # "explain": True,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"full_name_no_unique": {"query": raw_term, "boost": 1}}},
                    ],
                    # "minimum_should_match": 1,
                }
            },
            "_source": {"includes": ["full_name_no_unique"]},
            'from': 0,
            'size': rank_list_size
        }

        return j


    def search_term(self, raw_term, rank_list_size=10):
        j = self.get_match_term_query(raw_term, rank_list_size)
        res = self.es.search(index=self.index_name, body=j)
        return res  
    

    def self_match(self):
        def extract_name_and_score(res):
            return [(i['_source']['full_name_no_unique'], i['_score']) for i in res['hits']['hits']]
        
        ret_list = []
        for term in self.data:
            res = self.search_term(term, 10)
            ret_list.append(res)

        clean_pair_list = [extract_name_and_score(i) for i in ret_list]
        query_result_pair = list(zip(self.data, clean_pair_list))

        match_pair_list = list()
        for query, results in query_result_pair:
            for result in results:
                if query == result[0]:
                    continue
                else:
                    match_pair_list.append({
                        "query": query, 
                        "match": result[0],
                        "score": result[1]})
                    
        
        match_df = pd.DataFrame(match_pair_list)
        return match_df
    
    def compute_jacc_score(self, df):
        def set_intersection_score(text1, text2):
            set1 = set(re.findall(r'[\w\d]+', text1))
            set2 = set(re.findall(r'[\w\d]+', text2))
            return len(set1.intersection(set2)) / len(set1)
        
        # print(df)
        a = df.apply(lambda x: set_intersection_score(x['query'], x['match']), axis=1)
        # print(a)
        df['jacc_score'] = a
        return df
    

    def run_pipeline(self):
        match_df = self.self_match()
        ret = self.compute_jacc_score(match_df)
        return ret