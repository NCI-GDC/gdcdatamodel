from psqlgraph import Node, pg_property


class Archive(Node):

    __nonnull_properties__ = ['submitter_id', 'revision']

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(long, int)
    def revision(self, value):
        self._set_property('revision', value)
