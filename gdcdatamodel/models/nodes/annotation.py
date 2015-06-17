from psqlgraph import Node, pg_property
from node import *
from gdcdatamodel.models import validate


class Annotation(Node):

    __nonnull_properties__ = [
        'category',
        'classification',
        'creator',
        'created_datetime',
        'status',
    ]

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(str, enum=[
        'Acceptable treatment for TCGA tumor',
        'Administrative Compliance',
        'Alternate sample pipeline',
        'BCR Notification',
        'Barcode incorrect',
        'Biospecimen identity unknown',
        'Case submitted is found to be a recurrence after submission',
        'Center QC failed',
        'Clinical data insufficient',
        'Duplicate case',
        'Duplicate item',
        'General',
        'Genotype mismatch',
        'History of acceptable prior treatment related to a prior/other malignancy',
        'History of unacceptable prior treatment related to a prior/other malignancy',
        'Inadvertently shipped',
        'Item does not meet study protocol',
        'Item flagged DNU',
        'Item in special subset',
        'Item is noncanonical',
        'Item may not meet study protocol',
        'Molecular analysis outside specification',
        'Neoadjuvant therapy',
        'New notification type',
        'New observation type',
        'Normal class but appears diseased',
        'Normal tissue origin incorrect',
        'Pathology outside specification',
        'Permanently missing item or object',
        'Prior malignancy',
        'Qualification metrics changed',
        'Qualified in error',
        'Sample compromised',
        'Subject identity unknown',
        'Subject withdrew consent',
        'Synchronous malignancy',
        'Tumor class but appears normal',
        'Tumor tissue origin incorrect',
        'Tumor type incorrect',
        'WGA Failure'])
    def category(self, value):
        self._set_property('category', value)

    @pg_property(str)
    def classification(self, value):
        self._set_property('classification', value)

    @pg_property(str)
    def creator(self, value):
        self._set_property('creator', value)

    @pg_property(long, int)
    def created_datetime(self, value):
        self._set_property('created_datetime', value)

    @pg_property(str)
    def status(self, value):
        self._set_property('status', value)

    @pg_property(str)
    def notes(self, value):
        self._set_property('notes', value)
