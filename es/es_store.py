from elasticsearch import helpers, Elasticsearch
from abc import ABC

import json
import requests
import uuid
import asyncio
import time


class EsDoc(object):
    def __init__(self, raw_doc):
        self.raw_doc = raw_doc

    def __getattr__(self, item):
        if item == 'index':
            return self.raw_doc['_index']
        elif item == 'type':
            return self.raw_doc['_type']
        elif item == 'id':
            return self.raw_doc['_id']
        elif item == 'score':
            return self.raw_doc['_score']
        return self.raw_doc['_source'][item]

    def __str__(self):
        return json.dumps(self.raw_doc, ensure_ascii=False)


class EsStore(object):
    def __init__(self):
        self.es_addr = '127.0.0.1:9200'
        self.es_index = self.index_name()
        self.es_index_url = 'http://{addr}/{index}'.format(addr=self.es_addr, index=self.es_index)
        self.es_search_url = 'http://{addr}/{index}/_search?pretty=true'.format(addr=self.es_addr, index=self.es_index)

    def index_name(self):
        raise NotImplementedError

    def _query_count_on_index(self):
        resp = requests.get(self.es_index_url + '/_count').json()
        if 'count' in resp:
            return resp['count']
        return None

    def _query_mappings_on_index(self):
        resp = requests.get(self.es_index_url + '/_mapping?pretty=true')
        return resp

    def info(self):
        print('es addr={}'.format(self.es_addr))
        print('es index={}'.format(self.es_index))
        print('number of doc in this type={}'.format(self._query_count_on_index()))
        print('mapping of this type={}'.format(self._query_mappings_on_index().text))

    def mappings(self):
        raise NotImplementedError

    def set_mappings(self):
        requests.put(
            self.es_index_url,
            data=self.mappings(),
            headers={'Content-Type': 'application/json'},
        )

    def has_mappings(self):
        resp = self._query_mappings_on_index()
        if resp.status_code == 200:
            return True
        return False

    def doc_generator_for_file(self, file_path):
        raise NotImplementedError

    def build(self, file_path):
        self.set_mappings()
        self.append(self.doc_generator_for_file(file_path))

    def _doc_id(self, doc):
        return uuid.uuid1()

    def append(self, doc_generator, log_every_n_doc=1000):
        if not self.has_mappings():
            self.set_mappings()

        def _action_generator():
            for i, doc in enumerate(doc_generator):
                if (i + 1) % log_every_n_doc == 0:
                    print('creating {} doc ...'.format(i + 1))
                act = {
                    '_op_type': 'index',
                    '_index': self.es_index,
                    '_id': self._doc_id(doc),
                }
                act.update(doc)
                yield act

        es_client = Elasticsearch(hosts=self.es_addr)
        helpers.bulk(es_client, _action_generator())

    def remove(self):
        resp = requests.delete(self.es_index_url)

    def scan(self):
        es_client = Elasticsearch(hosts=self.es_addr)
        for es_resp in helpers.scan(
                es_client,
                query={"query": {"match_all": {}}},
                scroll='1024m',
                index=self.es_index
        ):
            yield EsDoc(es_resp)

    def _get_short_search_esult(self, es_resp):
        short_resp = []
        for hit in es_resp["hits"]["hits"]:
            inst = {
                'id': hit['_id'],
                'score': hit["_score"],
                'question': hit["_source"]["question"],
                'answer': hit["_source"]["answer"],
            }
            short_resp.append(inst)
        return short_resp

    def search(self, query, size=30):
        es_query = {
            "match": {
                "question": query,
            }
        }

        data = {
            "size": size,
            "query": es_query,
        }

        resp = requests.get(
            self.es_search_url,
            data=json.dumps(data),
            headers={'Content-Type': 'application/json'}
        ).json()

        duration = resp['took']
        short_resp = self._get_short_search_esult(resp)
        print('[done] call es_search in {} ms.'.format(duration))
        print('number of es_search result: {}'.format(len(short_resp)))
        return short_resp


class EsQA(EsStore, ABC):
    def index_name(self):
        return 'qa'

    def mappings(self):
        mappings = {
            'mappings': {
                'properties': {
                    'doc_id': {
                        'type': 'keyword'
                    },
                    'question': {
                        'type': 'text'
                    },
                    'answer': {
                        'type': 'text',
                    },
                }
            }
        }
        return json.dumps(mappings)

    def doc_generator_for_file(self, file_path):
        with open(file_path, 'r', encoding='utf8') as f:
            for line in f:
                line = json.loads(line.strip())
                question = line['question']
                answers = line['answer']
                if question and answers:
                    record = {
                        'question': question,
                        'answer': answers[0],
                    }
                    yield record


if __name__ == '__main__':
    file_path = 'data/es/test.txt'
    es = EsQA()
    es.build(file_path)
    es.info()
    es.remove()
