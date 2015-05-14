from psqlgraph import Edge


class RefersTo(object):
    __label__ = 'refers_to'


class PublicationRefersToFile(Edge, RefersTo):
    __src_class__ = 'Publication'
    __dst_class__ = 'File'
    __src_dst_assoc__ = 'files'
    __dst_src_assoc__ = 'publications'
