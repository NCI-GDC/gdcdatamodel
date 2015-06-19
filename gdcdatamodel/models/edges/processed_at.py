from psqlgraph import Edge, pg_property


class ProcessedAt(object):
    __label__ = 'processed_at'


class CaseProcessedAtTissueSourceSite(Edge, ProcessedAt):
    __src_class__ = 'Case'
    __src_table__ = '_case'
    __dst_class__ = 'TissueSourceSite'
    __src_dst_assoc__ = 'tissue_source_sites'
    __dst_src_assoc__ = 'cases'
