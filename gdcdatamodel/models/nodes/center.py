from node import *
from gdcdatamodel.models import validate


class Center(Node):

    __nonnull_properties__ = ['code', 'namespace', 'name',
                              'short_name', 'center_type']

    @hybrid_property
    def code(self):
        return self._get_property('code')

    @code.setter
    @validate(str)
    def code(self, value):
        self._get_property('code', value)

    @hybrid_property
    def namespace(self):
        return self._get_property('namespace')

    @namespace.setter
    @validate(str)
    def namespace(self, value):
        self._get_property('namespace', value)

    @hybrid_property
    def name(self):
        return self._get_property('name')

    @name.setter
    @validate(str)
    def name(self, value):
        self._get_property('name', value)

    @hybrid_property
    def short_name(self):
        return self._get_property('short_name')

    @short_name.setter
    @validate(str)
    def short_name(self, value):
        self._get_property('short_name', value)

    @hybrid_property
    def center_type(self):
        return self._get_property('center_type')

    @center_type.setter
    @validate(str)
    def center_type(self, value):
        self._get_property('center_type', value)
