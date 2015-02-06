import re
import requests
from cdisutils.log import get_logger
from datetime import datetime
from psqlgraph import Edge
from time import mktime, strptime
from uuid import UUID, uuid5

log = get_logger('tcga_annotations')
BASE_URL = "https://tcga-data.nci.nih.gov/annotations/resources/searchannotations/json"
DATE_RE = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(-\d{1,2}:\d{2})')
ANNOTATION_NS = 'e61d5a88-7f5c-488e-9c42-a5f32b4d1c50'
ITEMS = [None, 'aliquot', 'analyte', 'participant', 'portion', 'sample',
         'slide', 'portion']


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return long(delta.total_seconds())


class TCGAAnnotationImporter(object):

    def __init__(self, psqlgraphdriver):
        self.g = psqlgraphdriver

    def download_annotations(self, params={}, url=BASE_URL):
        """Downloads all annotations from TCGA

        """
        log.info('Downloading annotations from {} {}'.format(url, params))
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def from_url(self, *args, **kwargs):
        """start conversion by download and insert all annotations

        """
        self.from_json(self.download_annotations(*args, **kwargs))

    def from_json(self, doc):
        """Import json-like python object of annotations into graph

        """
        log.info('Found {} annotations.'.format(len(doc['dccAnnotation'])))
        with self.g.session_scope():
            map(self.insert_annotation, doc['dccAnnotation'])

    def insert_annotation(self, doc):
        """Insert a single annotation dict into graph

        """
        with self.g.session_scope():
            dsts = map(self.lookup_item_node, doc['items'])
            if set(dsts) == {None}:
                return
            for noteID, note in self.get_notes(doc).items():
                src_id = self.generate_uuid(self.get_submitter_id(doc)+noteID)
                self.g.node_merge(
                    node_id=src_id,
                    label='annotation',
                    properties=self.munge_annotation(doc, note))
                [self.add_edge(dst, src_id) for dst in dsts]

    def add_edge(self, dst, src_id):
        """Idempotently add an edge from annotation to item

        """
        if not dst:
            return
        edge = self.g.edge_lookup(src_id, dst.node_id, 'annotates').first()
        if not edge:
            self.g.edge_insert(Edge(src_id, dst.node_id, 'annotates'))

    def munge_annotation(self, doc, noteText):
        """Parse doc to get node properties

        """
        return {
            'submitter_id': self.get_submitter_id(doc),
            'category': self.get_category(doc),
            'classification': self.get_classification(doc),
            'creator': self.get_creator(doc),
            'created_datetime': self.get_created_datetime(doc),
            'status': self.get_status(doc),
            'notes': noteText,
        }

    def generate_uuid(self, key):
        """UUID generated from key=(target barcode + noteID)

        """
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
        return {str(n['noteId']): n['noteText'] for n in doc.get('notes', [])}

    def parse_datetime(self, text):
        return unix_time(datetime.fromtimestamp(mktime(strptime(
            text, '%Y-%m-%dT%H:%M:%S'))))

    def lookup_item_node(self, item):
        """Lookup node by barcode under it's supposed label.  If we can't find
        it, check again without label constraint and complain if we
        find it that way.

        """
        itype = ITEMS[item['itemType']['itemTypeId']]
        node = self.g.nodes().labels(itype).props(
            {'submitter_id': item['item']}).first()
        if not node:
            node = self.g.nodes().props({'submitter_id': item['item']}).first()
            if node:
                log.error('Annotation {} to {} under wrong label {}'.format(
                    itype, item['item']), node.label)
            else:
                log.warn('No {} {}'.format(itype, item['item']))
        return node
