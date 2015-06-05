from psqlgraph import Node, pg_property


class Center(Node):

    __nonnull_properties__ = [
        'code',
        'namespace',
        'name',
        'short_name',
        'center_type',
    ]

    @pg_property(str)
    def code(self, value):
        self._set_property('code', value)

    @pg_property(str)
    def namespace(self, value):
        self._set_property('namespace', value)

    @pg_property(str)
    def name(self, value):
        self._set_property('name', value)

    @pg_property(str)
    def short_name(self, value):
        self._set_property('short_name', value)

    @pg_property(str)
    def center_type(self, value):
        self._set_property('center_type', value)
