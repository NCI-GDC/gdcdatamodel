from psqlgraph import Node, pg_property


class Project(Node):

    __nonnull_properties__ = [
        'code', 'name', 'disease_type', 'state', 'primary_site']

    @pg_property(str)
    def code(self, value):
        self._set_property('code', value)

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)

    @pg_property(str)
    def disease_type(self, value):
        self._set_property('disease_type', value)

    @pg_property(str)
    def state(self, value):
        self._set_property('state', value)

    @pg_property(str)
    def primary_site(self, value):
        self._set_property('primary_site', value)
