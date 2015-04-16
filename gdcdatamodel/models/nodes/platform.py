from node import *
from gdcdatamodel.models import validate


class Platform(Node):

    __nonnull_properties__ = ['name']

    @hybrid_property
    def name(self):
        return self._get_property('name')

    @name.setter
    @validate(str)
    def name(self, value):
        self._set_property('name', value)
