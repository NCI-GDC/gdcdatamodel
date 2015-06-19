from psqlgraph import Edge


class Annotates(object):
    __label__ = 'annotates'


class AnnotationAnnotatesCase(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Case'
    __dst_table__ = '_case'
    __src_dst_assoc__ = 'cases'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesSample(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Sample'
    __src_dst_assoc__ = 'samples'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesSlide(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Slide'
    __src_dst_assoc__ = 'slides'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesPortion(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Portion'
    __src_dst_assoc__ = 'portions'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesAnalyte(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Analyte'
    __src_dst_assoc__ = 'analytes'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesAliquot(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'Aliquot'
    __src_dst_assoc__ = 'aliquots'
    __dst_src_assoc__ = 'annotations'


class AnnotationAnnotatesFile(Edge, Annotates):
    __src_class__ = 'Annotation'
    __dst_class__ = 'File'
    __src_dst_assoc__ = 'files'
    __dst_src_assoc__ = 'annotations'
