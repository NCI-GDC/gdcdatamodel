from node import *
from gdcdatamodel.models import validate


class Annotation(Node):

    __nonnull_properties__ = ['category', 'classification', 'creator',
                              'created_datetime', 'status', 'notes']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def category(self):
        return self._get_property('category')

    @category.setter
    @validate(str, enum=['Acceptable treatment for TCGA tumor',
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

    @hybrid_property
    def classification(self):
        return self._get_property('classification')

    @classification.setter
    @validate(str)
    def classification(self, value):
        self._set_property('classification', value)

    @hybrid_property
    def creator(self):
        return self._get_property('creator')

    @creator.setter
    @validate(str)
    def creator(self, value):
        self._set_property('creator', value)

    @hybrid_property
    def created_datetime(self):
        return self._get_property('created_datetime')

    @created_datetime.setter
    @validate(long, int)
    def created_datetime(self, value):
        self._set_property('created_datetime', value)

    @hybrid_property
    def status(self):
        return self._get_property('status')

    @status.setter
    @validate(str)
    def status(self, value):
        self._set_property('status', value)

    @hybrid_property
    def notes(self):
        return self._get_property('notes')

    @notes.setter
    @validate(str)
    def notes(self, value):
        self._set_property('notes', value)
