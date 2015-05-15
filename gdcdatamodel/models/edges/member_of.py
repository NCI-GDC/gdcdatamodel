from psqlgraph import Edge


class MemberOf(object):
    __label__ = 'member_of'


class ParticipantMemberOfProject(Edge, MemberOf):
    __src_class__ = 'Participant'
    __dst_class__ = 'Project'
    __src_dst_assoc__ = 'projects'
    __dst_src_assoc__ = 'participants'


class ProjectMemberOfProgram(Edge, MemberOf):
    __src_class__ = 'Project'
    __dst_class__ = 'Program'
    __src_dst_assoc__ = 'programs'
    __dst_src_assoc__ = 'projects'


class ArchiveMemberOfProject(Edge, MemberOf):
    __src_class__ = 'Archive'
    __dst_class__ = 'Project'
    __src_dst_assoc__ = 'projects'
    __dst_src_assoc__ = 'archives'


class FileMemberOfArchive(Edge, MemberOf):
    __src_class__ = 'File'
    __dst_class__ = 'Archive'
    __src_dst_assoc__ = 'archives'
    __dst_src_assoc__ = 'files'


class FileMemberOfExperimentalStrategy(Edge, MemberOf):
    __src_class__ = 'File'
    __dst_class__ = 'ExperimentalStrategy'
    __src_dst_assoc__ = 'experimental_strategies'
    __dst_src_assoc__ = 'files'


class FileMemberOfDataSubtype(Edge, MemberOf):
    __src_class__ = 'File'
    __dst_class__ = 'DataSubtype'
    __src_dst_assoc__ = 'data_subtypes'
    __dst_src_assoc__ = 'files'


class FileMemberOfDataFormat(Edge, MemberOf):
    __src_class__ = 'File'
    __dst_class__ = 'DataFormat'
    __src_dst_assoc__ = 'data_formats'
    __dst_src_assoc__ = 'files'


class FileMemeberOfTag(Edge, MemberOf):
    __src_class__ = 'File'
    __dst_class__ = 'Tag'
    __src_dst_assoc__ = 'tags'
    __dst_src_assoc__ = 'files'


class DataSubtypeMemberOfDataType(Edge, MemberOf):
    __src_class__ = 'DataSubtype'
    __dst_class__ = 'DataType'
    __src_dst_assoc__ = 'data_types'
    __dst_src_assoc__ = 'data_subtypes'
