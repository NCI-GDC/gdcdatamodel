from psqlgraph import Edge


class ProcessedAt(object):
    __label__ = 'processed_at'


class CaseProcessedAtTissueSourceSite(Edge, ProcessedAt):
    __src_class__ = 'Case'
    __dst_class__ = 'TissueSourceSite'
    __src_dst_assoc__ = 'tissue_source_sites'
    __dst_src_assoc__ = 'cases'
