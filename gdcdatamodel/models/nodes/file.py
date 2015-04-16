from node import *
from gdcdatamodel.models import validate


class File(Node):

    __nonnull_properties__ = ['file_name', 'file_size', 'md5sum', 'state']

    @hybrid_property
    def file_name(self):
        return self._get_property('file_name')

    @file_name.setter
    @validate(str)
    def file_name(self, value):
        self._set_property('file_name', value)

    @hybrid_property
    def submitter_id(self):
        return self._get_property('submitter_id')

    @submitter_id.setter
    @validate(str)
    def submitter_id(self, value):
        self._set_property('submitter_id', value)

    @hybrid_property
    def file_size(self):
        return self._get_property('file_size')

    @file_size.setter
    @validate(int, long)
    def file_size(self, value):
        self._set_property('file_size', value)

    @hybrid_property
    def md5sum(self):
        return self._get_property('md5sum')

    @md5sum.setter
    @validate(str)
    def md5sum(self, value):
        self._set_property('md5sum', value)

    @hybrid_property
    def state(self):
        return self._get_property('state')

    @state.setter
    @validate(str,
              enum=['submitted', 'uploading', 'uploaded', 'generating',
                    'validating', 'invalid', 'suppressed', 'redacted', 'live'])
    def state(self, value):
        self._set_property('state', value)

    @hybrid_property
    def state_comment(self):
        return self._get_property('state_comment')

    @state_comment.setter
    @validate(str)
    def state_comment(self, value):
        self._set_property('state_comment', value)
