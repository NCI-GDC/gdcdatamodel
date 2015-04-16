from node import *
from gdcdatamodel.models import validate


class TissueSourceSite(Node):
    __label__ = 'tissue_source_site'

    @hybrid_property
    def code(self):
        return self._get_property('code')

    @code.setter
    @validate(str)
    def code(self, value):
        self._set_property('code', value)

    @hybrid_property
    def name(self):
        return self._get_property('name')

    @name.setter
    @validate(str)
    def name(self, value):
        self._set_property('name', value)

    @hybrid_property
    def project(self):
        return self._get_property('project')

    @project.setter
    @validate(str)
    def project(self, value):
        self._set_property('project', value)

    @hybrid_property
    def bcr_id(self):
        return self._get_property('bcr_id')

    @bcr_id.setter
    @validate(str)
    def bcr_id(self, value):
        self._set_property('bcr_id', value)
