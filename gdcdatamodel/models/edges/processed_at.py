from edge import Edge


class ProcessedAt(object):
    __label__ = 'processed_at'


class ParticipantProcessedAtTissueSourceSite(Edge, ProcessedAt):
    __src_class__ = 'Participant'
    __dst_class__ = 'TissueSourceSite'
    __src_dst_assoc__ = 'tissue_source_sites'
    __dst_src_assoc__ = 'participants'
