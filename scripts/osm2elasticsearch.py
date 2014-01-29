"""
Simple script to import osm data into elasticsearch
"""

from __future__ import print_function

import sys
from functools import partial

from imposm.parser import OSMParser
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


INPUT_FILE = '/data/samu/OSM/trentino-alto-adige.pbf'
ELASTICSEARCH_URL = 'http://localhost:9200/'
ELASTICSEARCH_INDEX = 'osm_data2'


es = Elasticsearch(ELASTICSEARCH_URL)

COMMON_MAPPING = {
    'properties': {
        'osm_id': {'type': 'long'},
        'tags': {
            'type': 'object',
            # '_default_': {'type': 'string'},
        },
        'data': {'type': 'object', 'enabled': False},
    },
    'dynamic_templates': [{
        'tags_as_string': {
            'path_match': 'tags.*',
            'mapping': {
                'type': 'string',
                # 'index': 'not_analyzed',
            }
        }
    }]
}

ES_MAPPING = {
    'node': COMMON_MAPPING,
    'way': COMMON_MAPPING,
    'relation': COMMON_MAPPING,
}

try:
    es.indices.delete(index=ELASTICSEARCH_INDEX)
except NotFoundError:
    pass

es.indices.create(index=ELASTICSEARCH_INDEX)
for key, mapping in ES_MAPPING.iteritems():
    es.indices.put_mapping(index=ELASTICSEARCH_INDEX, doc_type=key,
                           body={key: mapping})


def object_to_elasticsearch(obj_type, objects):
    ## The object we get is a three-tuple
    # (982347, {'name': 'Somewhere', 'place': 'village'}, (-120.2, 23.21))
    # (87644, {'name': 'my way', 'highway': 'path'}, [123, 345, 567])
    # (87644, {'type': 'multipolygon'}, [(123, 'way', 'outer'), (234, 'way', 'inner')])  # noqa
    for obj in objects:
        print("Indexing:", obj_type, repr(obj)[:100])
        es.index(
            index=ELASTICSEARCH_INDEX,
            doc_type=obj_type,
            id=str(obj[0]),
            body={
                'osm_id': obj[0],
                'tags': obj[1],
                'data': obj[2],
            })


p = OSMParser(
    concurrency=4,
    ways_callback=partial(object_to_elasticsearch, 'way'),
    nodes_callback=partial(object_to_elasticsearch, 'node'),
    relations_callback=partial(object_to_elasticsearch, 'relation'))

p.parse(INPUT_FILE)
