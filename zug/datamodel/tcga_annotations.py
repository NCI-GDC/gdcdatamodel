import re
import requests
from cdisutils.log import get_logger
from datetime import datetime
from psqlgraph import Edge
from time import mktime, strptime
from uuid import UUID, uuid5


BASE_URL = "https://tcga-data.nci.nih.gov/annotations/resources/searchannotations/json"
DATE_RE = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(-\d{1,2}:\d{2})')
ANNOTATION_NAMESPACE = UUID('e61d5a88-7f5c-488e-9c42-a5f32b4d1c50')
ITEMS = [None, 'aliquot', 'analyte', 'participant', 'portion', 'sample',
         'slide', 'portion']


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return long(delta.total_seconds())


class TCGAAnnotationImporter(object):

    def __init__(self, graph):
        self.graph = graph
        self.log = get_logger('tcga_annotation_sync')

    def download_annotations(self, params={}, url=BASE_URL):
        """Downloads all annotations from TCGA
        """
        self.log.info('Downloading annotations from {} {}'.format(url, params))
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
        self.log.info('Found {} annotations.'.format(len(doc['dccAnnotation'])))
        with self.graph.session_scope():
            map(self.insert_annotation, doc['dccAnnotation'])

    def insert_annotation(self, doc):
        """Insert a single annotation dict into graph

        """
        with self.graph.session_scope():
            dsts = map(self.lookup_item_node, doc['items'])
            if set(dsts) == {None}:
                return
            for noteID, note in self.graphet_notes(doc).items():
                src_id = self.graphenerate_uuid(self.graphet_submitter_id(doc)+noteID)
                self.graph.node_merge(
                    node_id=src_id,
                    label='annotation',
                    properties=self.munge_annotation(doc, note))
                [self.add_edge(dst, src_id) for dst in dsts]

    def add_edge(self, dst, src_id):
        """Idempotently add an edge from annotation to item

        """
        if not dst:
            return
        edge = self.graph.edge_lookup(src_id, dst.node_id, 'annotates').first()
        if not edge:
            self.graph.edge_insert(Edge(src_id, dst.node_id, 'annotates'))

    def munge_annotation(self, doc, noteText):
        """Parse doc to get node properties

        """
        return {
            'submitter_id': self.graphet_submitter_id(doc),
            'category': self.graphet_category(doc),
            'classification': self.graphet_classification(doc),
            'creator': self.graphet_creator(doc),
            'created_datetime': self.graphet_created_datetime(doc),
            'status': self.graphet_status(doc),
            'notes': noteText,
        }

    def generate_uuid(self, key):
        """UUID generated from key=(target barcode + noteID)

        """
        return str(uuid5(ANNOTATION_NAMESPACE, key))

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
        node = self.graph.nodes().labels(itype).props(
            {'submitter_id': item['item']}).first()
        if not node:
            node = self.graph.nodes().props({'submitter_id': item['item']}).first()
            if node:
                self.log.error('Annotation {} to {} under wrong label {}'.format(
                    itype, item['item']), node.label)
            else:
                self.log.warn('No {} {}'.format(itype, item['item']))
        return node
