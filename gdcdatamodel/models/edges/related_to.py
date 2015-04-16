from edge import Edge


class RelatedTo(object):
    __label__ = 'related_to'


class FileRelatedToFile(Edge, RelatedTo):
    __src_label__ = 'file'
    __dst_label__ = 'file'


class ArchiveRelatedToFile(Edge, RelatedTo):
    __src_label__ = 'archive'
    __dst_label__ = 'file'
