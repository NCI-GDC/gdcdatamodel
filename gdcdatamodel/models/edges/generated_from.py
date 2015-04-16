from edge import Edge


class GeneratedFrom(object):
    __label__ = 'generated_from'


class FileGeneratedFromPlatform(Edge, GeneratedFrom):
    __src_label__ = 'file'
    __dst_label__ = 'platform'
