from psqlgraph import Node, pg_property


class File(Node):

    __nonnull_properties__ = [
        'file_name',
        'file_size',
        'md5sum',
        'state'
    ]

    @pg_property(str)
    def file_name(self, value):
        self._set_property('file_name', value)

    @pg_property(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @pg_property(int, long)
    def file_size(self, value):
        self._set_property('file_size', value)

    @pg_property(str)
    def md5sum(self, value):
        self._set_property('md5sum', value)

    @pg_property(str, enum=[
        'submitted',
        'uploading',
        'uploaded',
        'generating',
        'validating',
        'invalid',
        'suppressed',
        'redacted',
        'live'])
    def state(self, value):
        self._set_property('state', value)

    @pg_property(str)
    def state_comment(self, value):
        self._set_property('state_comment', value)
