from psqlgraph import Edge


class DerivedFrom(object):
    __label__ = 'derived_from'


class AliquotDerivedFromAnalyte(Edge,  DerivedFrom):
    __src_class__ = 'Aliquot'
    __dst_class__ = 'Analyte'
    __src_dst_assoc__ = 'analytes'
    __dst_src_assoc__ = 'aliquots'


class AliquotDerivedFromSample(Edge,  DerivedFrom):
    __src_class__ = 'Aliquot'
    __dst_class__ = 'Sample'
    __src_dst_assoc__ = 'samples'
    __dst_src_assoc__ = 'aliquots'


class AnalyteDerivedFromPortion(Edge, DerivedFrom):
    __src_class__ = 'Analyte'
    __dst_class__ = 'Portion'
    __src_dst_assoc__ = 'portions'
    __dst_src_assoc__ = 'analytes'


class PortionDerivedFromSample(Edge, DerivedFrom):
    __src_class__ = 'Portion'
    __dst_class__ = 'Sample'
    __src_dst_assoc__ = 'samples'
    __dst_src_assoc__ = 'portions'


class SampleDerivedFromCase(Edge, DerivedFrom):
    __src_class__ = 'Sample'
    __dst_class__ = 'Case'
    __dst_table__ = '_case'
    __src_dst_assoc__ = 'cases'
    __dst_src_assoc__ = 'samples'


class SlideDerivedFromPortion(Edge, DerivedFrom):
    __src_class__ = 'Slide'
    __dst_class__ = 'Portion'
    __src_dst_assoc__ = 'portions'
    __dst_src_assoc__ = 'slides'
