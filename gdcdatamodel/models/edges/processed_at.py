from edge import Edge


class ProcessedAt(object):
    __label__ = 'processed_at'


class ParticipantProcessedAtTissueSourceSite(Edge, ProcessedAt):
    __src_label__ = 'participant'
    __dst_label__ = 'tissuesourcesite'
