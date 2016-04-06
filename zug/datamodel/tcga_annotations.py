import re
import os
import requests
from cdisutils.log import get_logger
from datetime import datetime
from time import mktime, strptime
from uuid import UUID, uuid5
from psqlgraph import PsqlGraphDriver

from psqlgraph import Node

from gdcdatamodel.models import (
    File, Aliquot, Analyte, Slide,
    Sample, Portion, Case,
    Annotation
)


BASE_URL = "https://tcga-data.nci.nih.gov/annotations/resources/searchannotations/json"

DATE_RE = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(-\d{1,2}:\d{2})')

ANNOTATION_NAMESPACE = UUID('e61d5a88-7f5c-488e-9c42-a5f32b4d1c50')



class TCGAAnnotationSyncer(object):

    def __init__(self):
        self.graph = PsqlGraphDriver(
            os.environ["PG_HOST"],
            os.environ["PG_USER"],
            os.environ["PG_PASS"],
            os.environ["PG_NAME"],
        )
        self.log = get_logger('tcga_annotation_sync')

    def download_annotations(self, url=BASE_URL):
        """Downloads all annotations from TCGA
        """
        self.log.info('Downloading annotations from %s', url)
        resp = requests.get(url)
        resp.raise_for_status()
        try:
            annotation_docs = resp.json()["dccAnnotation"]
        except:
            self.log.error("Unable to parse response as JSON")
            self.log.error(resp.text)
            raise
        # sanity checks
        for doc in annotation_docs:
            assert len(doc["items"]) == 1
        item_types = {doc["items"][0]["itemType"]["itemTypeName"].lower()
                      for doc in annotation_docs}
        assert item_types.issubset({"file", "patient", "aliquot", "analyte",
                                    "portion", "shipped portion", "slide",
                                    "sample"})
        return annotation_docs

    def go(self):
        annotation_docs = self.download_annotations()
        with self.graph.session_scope():
            for annotation_doc in annotation_docs:
                self.insert_annotation(annotation_doc)

    def insert_annotation(self, doc):
        """Insert a single annotation dict into graph
        """
        item = doc["items"][0]
        dst = self.lookup_item_node(item)
        if not dst:
            self.log.info("No item found for annotation %s, skipping", doc["id"])
            return
        for note_id, note_text in self.get_notes(doc).items():
            annotation = Annotation(
                node_id=self.generate_uuid(self.get_submitter_id(doc)+note_id),
                submitter_id=self.get_submitter_id(doc),
                category=self.get_category(doc),
                classification=self.get_classification(doc),
                creator=self.get_creator(doc),
                created_datetime=self.get_created_datetime(doc),
                status=self.get_status(doc),
                notes=note_text,
            )
            annotation.acl = ["open"]

            # Add/update noded in database
            if annotation.node_id in {n.node_id for n in dst.annotations}:
                self.log.info(
                    "%s already has %s, updating annotation", dst, annotation)
                self.graph.current_session().merge(annotation)
            else:
                self.log.info(
                    "inserting annotation %s tied to %s", annotation, dst)
                # Doing the assignment adds it to the session, so it gets
                # persisted when we flush
                dst.annotations.append(annotation)

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
        notes = {str(n['noteId']): n['noteText'] for n in doc.get('notes', [])}
        if not notes:
            # If there are no notes, we want to add one in with a null
            # note string, this way an annotation will still be
            # created for annotations that do not have associated
            # notes
            notes = {'': None}
        return notes

    def parse_datetime(self, text):
        return datetime.fromtimestamp(mktime(strptime(
            text, '%Y-%m-%dT%H:%M:%S'))).isoformat('T')

    def lookup_item_node(self, item):
        """Lookup node by barcode under it's supposed label.  If we can't find
        it, check again without label constraint and complain if we
        find it that way.

        """
        item_type = item['itemType']['itemTypeName'].lower()
        if item_type == "shipped portion":
            item_type = "portion"  # we just call shipped portions portions
        if item_type == "patient":
            item_type = "case"  # they say patient, we say case. this will have to be case eventually
        cls = Node.get_subclass(item_type)
        # TODO: Watch for project_id to be filled and use that to query,
        # also possibly use the value in group_id, but presence is good
        # for now
        try:
            node = self.graph.nodes(cls)\
                             .props({'submitter_id': item['item']})\
                             .filter(cls._sysan.has_key('group_id'))\
                             .filter(cls._sysan.has_key('version'))\
                             .scalar()
        except:
            self.log.error("Multiple project_ids found for %s" %
                    item['item'])
            raise

        return node
