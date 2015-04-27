from psqlgraph import Node, pg_property


class TissueSourceSite(Node):
    __label__ = 'tissue_source_site'

    @pg_property(str)
    def code(self, value):
        self._set_property('code', value)

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)

    @pg_property(str)
    def project(self, value):
        self._set_property('project', value)

    @pg_property(str)
    def bcr_id(self, value):
        self._set_property('bcr_id', value)
