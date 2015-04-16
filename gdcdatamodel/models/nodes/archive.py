from node import *
from gdcdatamodel.models import validate


class Archive(Node):

    __nonnull_properties__ = ['submitter_id', 'revision']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def revision(self):
        return self._get_property('revision')

    @revision.setter
    @validate(int, long)
    def revision(self, value):
        self._set_property('revision', value)
