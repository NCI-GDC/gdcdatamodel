from edge import Edge


class DerivedFrom(object):
    __label__ = 'derived_from'


class AliquotDerivedFromAnalyte(Edge,  DerivedFrom):
    __src_label__ = 'aliquot'
    __dst_label__ = 'analyte'


class AliquotDerivedFromSample(Edge,  DerivedFrom):
    __src_label__ = 'aliquot'
    __dst_label__ = 'sample'


class AnalyteDerivedFromPortion(Edge, DerivedFrom):
    __src_label__ = 'analyte'
    __dst_label__ = 'portion'


class PortionDerivedFromSample(Edge, DerivedFrom):
    __src_label__ = 'portion'
    __dst_label__ = 'sample'


class SampleDerivedFromParticipant(Edge, DerivedFrom):
    __src_label__ = 'sample'
    __dst_label__ = 'participant'


class SlideDerivedFromPortion(Edge, DerivedFrom):
    __src_label__ = 'slide'
    __dst_label__ = 'portion'
