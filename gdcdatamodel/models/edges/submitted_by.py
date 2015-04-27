from psqlgraph import Edge


class SubmittedBy(object):
    __label__ = 'submitted_by'


class FileSubmittedByCenter(Edge, SubmittedBy):
    __src_class__ = 'File'
    __dst_class__ = 'Center'
    __src_dst_assoc__ = 'centers'
    __dst_src_assoc__ = 'files'
