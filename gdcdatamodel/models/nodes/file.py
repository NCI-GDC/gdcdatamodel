from node import *

class File(Node):
    @hybrid_property
    def file_name(self):
        return self.properties['file_name']

    @file_name.setter
    def file_name(self, value):
        self.properties['file_name'] = value

    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def file_size(self):
        return self.properties['file_size']

    @file_size.setter
    def file_size(self, value):
        self.properties['file_size'] = value

    @hybrid_property
    def md5sum(self):
        return self.properties['md5sum']

    @md5sum.setter
    def md5sum(self, value):
        self.properties['md5sum'] = value

    @hybrid_property
    def state(self):
        return self.properties['state']

    @state.setter
    def state(self, value):
        self.properties['state'] = value

    @hybrid_property
    def state_comment(self):
        return self.properties['state_comment']

    @state_comment.setter
    def state_comment(self, value):
        self.properties['state_comment'] = value
