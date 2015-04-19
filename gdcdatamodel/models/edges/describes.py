from edge import Edge


class Describes(object):
    __label__ = 'describes'


class FileDescribesParticipant(Edge, Describes):
    __src_class__ = 'File'
    __dst_class__ = 'Participant'
    __src_dst_assoc__ = 'described_participants'
    __dst_src_assoc__ = 'describing_files'


class ClinicalDescribesParticipant(Edge, Describes):
    __src_class__ = 'Clinical'
    __dst_class__ = 'Participant'
    __src_dst_assoc__ = 'participants'
    __dst_src_assoc__ = 'clinicals'
