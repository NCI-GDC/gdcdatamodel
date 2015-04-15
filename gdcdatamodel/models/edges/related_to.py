from edge import *


class RelatedTo(object):
    __label__ = 'related_to'


class FileRelatedToFile(object):
    __src_label__ = 'file'
    __dst_label__ = 'file'


class ArchiveRelatedToFile(object):
    __src_label__ = 'archive'
    __dst_label__ = 'file'
