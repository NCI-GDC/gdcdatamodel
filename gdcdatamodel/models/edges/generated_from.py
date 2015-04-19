from edge import Edge


class GeneratedFrom(object):
    __label__ = 'generated_from'


class FileGeneratedFromPlatform(Edge, GeneratedFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Platform'
    __src_dst_assoc__ = 'platforms'
    __dst_src_assoc__ = 'files'
