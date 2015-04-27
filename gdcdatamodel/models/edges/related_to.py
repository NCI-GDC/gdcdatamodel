from psqlgraph import Edge


class RelatedTo(object):
    __label__ = 'related_to'


class FileRelatedToFile(Edge, RelatedTo):
    __src_class__ = 'File'
    __dst_class__ = 'File'
    __src_dst_assoc__ = 'related_files'
    __dst_src_assoc__ = 'parent_files'


class ArchiveRelatedToFile(Edge, RelatedTo):
    __src_class__ = 'Archive'
    __dst_class__ = 'File'
    __src_dst_assoc__ = 'files'
    __dst_src_assoc__ = 'archives'
