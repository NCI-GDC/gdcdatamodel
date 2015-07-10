from psqlgraph import Edge


class DataFrom(object):
    __label__ = 'data_from'


class FileDataFromAliquot(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Aliquot'
    __src_dst_assoc__ = 'aliquots'
    __dst_src_assoc__ = 'files'


class FileDataFromAnalyte(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Analyte'
    __src_dst_assoc__ = 'analytes'
    __dst_src_assoc__ = 'files'


class FileDataFromPortion(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Portion'
    __src_dst_assoc__ = 'portions'
    __dst_src_assoc__ = 'files'


class FileDataFromSample(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Sample'
    __src_dst_assoc__ = 'samples'
    __dst_src_assoc__ = 'files'


class FileDataFromCase(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Case'
    __src_dst_assoc__ = 'cases'
    __dst_src_assoc__ = 'files'


class FileDataFromSlide(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'Slide'
    __src_dst_assoc__ = 'slides'
    __dst_src_assoc__ = 'files'


class FileDataFromFile(Edge, DataFrom):
    __src_class__ = 'File'
    __dst_class__ = 'File'
    __src_dst_assoc__ = 'derived_files'
    __dst_src_assoc__ = 'source_files'
