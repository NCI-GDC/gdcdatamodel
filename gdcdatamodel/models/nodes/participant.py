from node import *
from gdcdatamodel.models import validate


class Participant(Node):

    __nonnull_properties__ = ['submitter_id']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def days_to_index(self):
        return self._get_property('days_to_index')

    @days_to_index.setter
    @validate(int)
    def days_to_index(self, value):
        self._set_property('days_to_index', value)
