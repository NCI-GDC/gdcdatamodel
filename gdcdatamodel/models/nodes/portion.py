from node import *
from gdcdatamodel.models import validate


class Portion(Node):

    __nonnull_properties__ = ['submitter_id', 'portion_number',
                              'creation_datetime']

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def portion_number(self):
        return self._get_property('portion_number')

    @portion_number.setter
    @validate(str)
    def portion_number(self, value):
        self._set_property('portion_number', value)

    @hybrid_property
    def creation_datetime(self):
        return self._get_property('creation_datetime')

    @creation_datetime.setter
    @validate(long, int)
    def creation_datetime(self, value):
        self._set_property('creation_datetime', value)

    @hybrid_property
    def weight(self):
        return self._get_property('weight')

    @weight.setter
    @validate(float)
    def weight(self, value):
        self._set_property('weight', value)

    @hybrid_property
    def is_ffpe(self):
        return self._get_property('is_ffpe')

    @is_ffpe.setter
    @validate(bool)
    def is_ffpe(self, value):
        self._set_property('is_ffpe', value)
