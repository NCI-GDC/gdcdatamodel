from edge import Edge


class Annotates(object):
    __label__ = 'annotates'


class AnnotationAnnotatesParticipant(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'participant'


class AnnotationAnnotatesSample(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'sample'


class AnnotationAnnotatesSlide(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'slide'


class AnnotationAnnotatesPortion(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'portion'


class AnnotationAnnotatesAnalyte(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'analyte'


class AnnotationAnnotatesAliquot(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'aliquot'


class AnnotationAnnotatesFile(Edge, Annotates):
    __src_label__ = 'annotation'
    __dst_label__ = 'file'
