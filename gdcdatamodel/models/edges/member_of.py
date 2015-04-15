from edge import *


class MemberOf(object):
    __label__ = 'member_of'


class FileMemberOfArchive(object):
    __src_label__ = 'file'
    __dst_label__ = 'archive'


class ParticipantMemberOfProject(object):
    __src_label__ = 'participant'
    __dst_label__ = 'project'


class ProjectMemberOfProgram(object):
    __src_label__ = 'project'
    __dst_label__ = 'program'


class ArchiveMemberOfProject(object):
    __src_label__ = 'archive'
    __dst_label__ = 'project'


class FileMemberOfExperimentalStrategy(object):
    __src_label__ = 'file'
    __dst_label__ = 'experimental_strategy'


class FileMemberOfDataSubtype(object):
    __src_label__ = 'file'
    __dst_label__ = 'data_subtype'


