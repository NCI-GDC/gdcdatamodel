from edge import Edge


class SubmittedBy(object):
    __label__ = 'submitted_by'


class FileSubmittedByCenter(Edge, SubmittedBy):
    __src_label__ = 'file'
    __dst_label__ = 'center'
