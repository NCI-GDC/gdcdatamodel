from node import *
from gdcdatamodel.models import validate


class Aliquot(Node):

    __nonnull_properties__ = ['submitter_id']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def source_center(self):
        return self._get_property('source_center')

    @source_center.setter
    @validate(str)
    def source_center(self, value):
        self._set_property('source_center', value)

    @hybrid_property
    def amount(self):
        return self._get_property('amount')

    @amount.setter
    @validate(float)
    def amount(self, value):
        self._set_property('amount', value)

    @hybrid_property
    def concentration(self):
        return self._get_property('concentration')

    @concentration.setter
    @validate(float)
    def concentration(self, value):
        self._set_property('concentration', value)
