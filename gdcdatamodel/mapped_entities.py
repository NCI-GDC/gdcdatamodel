from addict import Dict

# Correspondence values
ONE_TO_ONE = '__one_to_one__'
ONE_TO_MANY = '__one_to_many__'

# File hierarchy
file_tree = Dict()
file_tree.corr = (ONE_TO_MANY, 'files')
file_tree.annotation.corr = (ONE_TO_MANY, 'annotations')
file_tree.archive.corr = (ONE_TO_ONE, 'archive')
file_tree.center.corr = (ONE_TO_ONE, 'center')
file_tree.data_format.corr = (ONE_TO_ONE, 'data_format')
file_tree.data_subtype.corr = (ONE_TO_ONE, 'data_subtype')
file_tree.data_subtype.data_type.corr = (ONE_TO_ONE, 'data_type')
file_tree.experimental_strategy.corr = (ONE_TO_ONE, 'experimental_strategy')
file_tree.case.corr = (ONE_TO_MANY, 'cases')
file_tree.platform.corr = (ONE_TO_ONE, 'platform')
file_tree.tag.corr = (ONE_TO_MANY, 'tags')
file_tree.file.corr = (ONE_TO_MANY, 'related_files')

file_traversal = Dict()
file_traversal.center = [('center'), ('aliquot', 'center')]
file_traversal.case = [
    ('sample', 'case'),
    ('file', 'sample', 'case'),
    ('aliquot', 'sample', 'case'),
    ('file', 'aliquot', 'sample', 'case'),
    ('analyte', 'portion', 'sample', 'case'),
    ('file', 'analyte', 'portion', 'sample', 'case'),
    ('aliquot', 'analyte', 'portion', 'sample', 'case'),
    ('file', 'aliquot', 'analyte', 'portion', 'sample', 'case'),
]

# Participant hierarchy
case_tree = Dict()
case_tree.corr = (ONE_TO_MANY, 'cases')
case_tree.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.clinical.corr = (ONE_TO_ONE, 'clinical')
case_tree.project.corr = (ONE_TO_ONE, 'project')
case_tree.project.program.corr = (ONE_TO_ONE, 'program')
case_tree.sample.corr = (ONE_TO_MANY, 'samples')
case_tree.sample.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.sample.portion.corr = (ONE_TO_MANY, 'portions')
case_tree.sample.portion.analyte.corr = (ONE_TO_MANY, 'analytes')
case_tree.sample.portion.analyte.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.sample.portion.analyte.aliquot.corr = (ONE_TO_MANY, 'aliquots')
case_tree.sample.portion.analyte.aliquot.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.sample.portion.analyte.aliquot.center.corr = (ONE_TO_ONE, 'center')
case_tree.sample.portion.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.sample.portion.center.corr = (ONE_TO_ONE, 'center')
case_tree.sample.portion.slide.corr = (ONE_TO_MANY, 'slides')
case_tree.sample.portion.slide.annotation.corr = (ONE_TO_MANY, 'annotations')
case_tree.tissue_source_site.corr = (ONE_TO_ONE, 'tissue_source_site')
case_tree.file.corr = (ONE_TO_MANY, 'files')

# for target
case_tree.aliquot = case_tree.sample.portion.analyte.aliquot
case_tree.sample.aliquot = case_tree.sample.portion.analyte.aliquot

case_traversal = Dict()
case_traversal.file = [
    ('sample', 'file'),
    ('sample', 'aliquot', 'file'),
    ('sample', 'portion', 'analyte', 'file'),
    ('sample', 'portion', 'analyte', 'aliquot', 'file'),
]

# Annotation hierarchy
annotation_tree = Dict()
annotation_tree.corr = (ONE_TO_MANY, 'cases')
annotation_tree.project.corr = (ONE_TO_ONE, 'project')
annotation_tree.project.program.corr = (ONE_TO_ONE, 'program')
annotation_tree.case.corr = (ONE_TO_ONE, 'case')
annotation_tree.sample.corr = (ONE_TO_ONE, 'sample')
annotation_tree.portion.corr = (ONE_TO_ONE, 'portion')
annotation_tree.analyte.corr = (ONE_TO_ONE, 'analyte')
annotation_tree.aliquot.corr = (ONE_TO_ONE, 'aliquot')
annotation_tree.slide.corr = (ONE_TO_ONE, 'slide')
annotation_tree.file.corr = (ONE_TO_ONE, 'file')


annotation_traversal = Dict()
annotation_traversal.file = [
    ('sample', 'file'),
    ('sample', 'file', 'file'),
    ('analyte', 'file'),
    ('analyte', 'file', 'file'),
    ('case', 'file'),
    ('case', 'file', 'file'),
    ('sample', 'aliquot', 'file'),
    ('sample', 'aliquot', 'file', 'file'),
    ('sample', 'portion', 'analyte', 'file'),
    ('sample', 'portion', 'analyte', 'file', 'file'),
    ('sample', 'portion', 'analyte', 'aliquot', 'file'),
    ('sample', 'portion', 'analyte', 'aliquot', 'file', 'file'),
]

# Project hierarchy
project_tree = Dict()
project_tree.corr = (ONE_TO_ONE, 'project')
project_tree.program.corr = (ONE_TO_ONE, 'program')
