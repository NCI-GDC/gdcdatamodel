from psqlgraph import Node, pg_property


class Publication(Node):

    __nonnull_properties__ = ['pmid', 'doi']

    @pg_property(str)
    def pmid(self, value):
        self._set_property('pmid', value)

    @pg_property(str)
    def doi(self, value):
        self._set_property('doi', value)
