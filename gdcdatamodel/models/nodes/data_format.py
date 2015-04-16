from node import *
from gdcdatamodel.models import validate


class DataFormat(Node):

    __label__ = 'data_format'
    __nonnull_properties__ = ['name']

    @hybrid_property
    def name(self):
        return self._get_property('name')

    @name.setter
    @validate(str)
    def name(self, value):
        self._set_property('name', value)
