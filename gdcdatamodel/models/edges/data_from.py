from edge import Edge


class DataFrom(object):
    __label__ = 'data_from'


class FileDataFromAliquot(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'aliquot'


class FileDataFromAnalyte(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'analyte'


class FileDataFromPortion(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'portion'


class FileDataFromSample(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'sample'


class FileDataFromParticipant(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'participant'


class FileDataFromSlide(Edge, DataFrom):
    __src_label__ = 'file'
    __dst_label__ = 'slide'
