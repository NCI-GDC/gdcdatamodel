from node import *
from gdcdatamodel.models import validate


class Project(Node):

    __nonnull_properties__ = ['code', 'name', 'disease_type', 'state',
                              'primary_site']
    __children__ = {
        'programs': 'ProjectMemberOfProgram'
    }

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
    def disease_type(self):
        return self._get_property('disease_type')

    @disease_type.setter
    @validate(str)
    def disease_type(self, value):
        self._set_property('disease_type', value)

    @hybrid_property
    def state(self):
        return self._get_property('state')

    @state.setter
    @validate(str)
    def state(self, value):
        self._set_property('state', value)

    @hybrid_property
    def primary_site(self):
        return self._get_property('primary_site')

    @primary_site.setter
    @validate(str)
    def primary_site(self, value):
        self._set_property('primary_site', value)
