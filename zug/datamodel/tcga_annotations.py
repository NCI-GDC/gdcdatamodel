import json
import re
import requests
from cdisutils.log import get_logger
from datetime import datetime
from psqlgraph import Edge
from time import mktime, strptime
from uuid import UUID, uuid5

log = get_logger("tcga_annotation_importer")
BASE_URL = "https://tcga-data.nci.nih.gov/annotations/resources/searchannotations/json"
DATE_RE = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(-\d{1,2}:\d{2})')
ANNOTATION_NS = 'e61d5a88-7f5c-488e-9c42-a5f32b4d1c50'


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return long(delta.total_seconds())


class TCGAAnnotationImporter(object):

    def __init__(self, psqlgraphdriver):
        self.g = psqlgraphdriver

    def download_annotations(self, params={}, url=BASE_URL):
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def from_url(self, *args, **kwargs):
        self.from_json(self.download_annotations(*args, **kwargs))

    def from_json(self, doc):
        log.info('Found {} annotations.'.format(len(doc['dccAnnotation'])))
        with self.g.session_scope():
            map(self.insert_annotation, doc['dccAnnotation'])

    def insert_annotation(self, doc):
        props = self.munge_annotation(doc)
        item = self.lookup_item_node(doc)
        node_id = self.generate_uuid(props['submitter_id'])
        if not item:
            return
        self.g.node_merge(
            node_id=node_id, label='annotation', properties=props)
        edge = self.g.edge_lookup(node_id, item.node_id, 'annotates').first()
        if not edge:
            self.g.edge_insert(Edge(node_id, item.node_id, 'annotates'))

    def munge_annotation(self, doc):
        return {
            'submitter_id': self.get_submitter_id(doc),
            'category': self.get_category(doc),
            'classification': self.get_classification(doc),
            'creator': self.get_creator(doc),
            'created_datetime': self.get_created_datetime(doc),
            'status': self.get_status(doc),
            'notes': self.get_notes(doc),
        }

    def generate_uuid(self, key):
        return str(uuid5(UUID(ANNOTATION_NS), key))

    def get_category(self, doc):
        return doc['annotationCategory']['categoryName']

    def get_submitter_id(self, doc):
        return str(doc['id'])

    def get_classification(self, doc):
        return doc[
            'annotationCategory'][
                'annotationClassification'][
                    'annotationClassificationName']

    def get_creator(self, doc):
        return doc['createdBy']

    def get_created_datetime(self, doc):
        return self.parse_datetime(DATE_RE.match(doc['dateCreated']).group(1))

    def get_status(self, doc):
        return doc['status']

    def get_notes(self, doc):
        assert len(doc['notes']) <= 1
        return doc['notes'][0]['noteText']

    def parse_datetime(self, text):
        return unix_time(datetime.fromtimestamp(mktime(strptime(
            text, '%Y-%m-%dT%H:%M:%S'))))

    def lookup_item_node(self, doc):
        assert len(doc['notes']) <= 1
        sid = doc['items'][0]['item']
        node = self.g.nodes().props({'submitter_id': sid}).first()
        if not node:
            log.warn('Unable to find item {} for annotation {}'.format(
                sid, doc['id']))
        return node
