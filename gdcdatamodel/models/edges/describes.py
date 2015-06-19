from psqlgraph import Edge


class Describes(object):
    __label__ = 'describes'


class FileDescribesCase(Edge, Describes):
    __src_class__ = 'File'
    __dst_class__ = 'Case'
    __dst_table__ = '_case'
    __src_dst_assoc__ = 'described_cases'
    __dst_src_assoc__ = 'describing_files'


class ClinicalDescribesCase(Edge, Describes):
    __src_class__ = 'Clinical'
    __dst_class__ = 'Case'
    __dst_table__ = '_case'
    __src_dst_assoc__ = 'cases'
    __dst_src_assoc__ = 'clinicals'
