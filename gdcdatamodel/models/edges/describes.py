from edge import Edge


class Describes(object):
    __label__ = 'describes'


class FileDescribesParticipant(Edge, Describes):
    __src_label__ = 'file'
    __dst_label__ = 'participant'


class ClinicalDescribesParticipant(Edge, Describes):
    __src_label__ = 'clinical'
    __dst_label__ = 'participant'
