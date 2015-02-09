from addict import Dict

# Correspondence values
ONE_TO_ONE = '__one_to_one__'
ONE_TO_MANY = '__one_to_many__'

# File hierarchy
file_tree = Dict()
file_tree.annotation.corr = (ONE_TO_MANY, 'annotations')
file_tree.archive.corr = (ONE_TO_MANY, 'archives')
file_tree.center.corr = (ONE_TO_ONE, 'centers')
file_tree.data_format.corr = (ONE_TO_ONE, 'data_format')
file_tree.data_subtype.corr = (ONE_TO_ONE, 'data_type')
file_tree.data_subtype.data_type.corr = (ONE_TO_ONE, 'data_type')
file_tree.experimental_strategy.corr = (ONE_TO_ONE, 'experimental_strategy')
file_tree.participant.corr = (ONE_TO_MANY, 'participants')
file_tree.platform.corr = (ONE_TO_ONE, 'platform')
file_tree.tag.corr = (ONE_TO_MANY, 'tags')

file_traversal = Dict()
file_traversal.center = [('center'), ('aliquot', 'center')]
file_traversal.participant = [
    ('sample', 'participant'),
    ('file', 'sample', 'participant'),
    ('aliquot', 'sample', 'participant'),
    ('file', 'aliquot', 'sample', 'participant'),
    ('analyte', 'portion', 'sample', 'participant'),
    ('file', 'analyte', 'portion', 'sample', 'participant'),
    ('aliquot', 'analyte', 'portion', 'sample', 'participant'),
    ('file', 'aliquot', 'analyte', 'portion', 'sample', 'participant'),
]

# Participant hierarchy
participant_tree = Dict()
participant_tree.corr = (ONE_TO_MANY, 'participants')
participant_tree.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.clinical.corr = (ONE_TO_ONE, 'clinical')
participant_tree.project.corr = (ONE_TO_ONE, 'project')
participant_tree.project.program.corr = (ONE_TO_ONE, 'program')
participant_tree.sample.corr = (ONE_TO_MANY, 'samples')
participant_tree.sample.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.sample.portion.corr = (ONE_TO_MANY, 'portions')
participant_tree.sample.portion.analyte.corr = (ONE_TO_MANY, 'analytes')
participant_tree.sample.portion.analyte.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.sample.portion.analyte.aliquot.corr = (ONE_TO_MANY, 'aliquots')
participant_tree.sample.portion.analyte.aliquot.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.sample.portion.analyte.aliquot.center.corr = (ONE_TO_ONE, 'center')
participant_tree.sample.portion.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.sample.portion.center.corr = (ONE_TO_ONE, 'center')
participant_tree.sample.portion.slide.corr = (ONE_TO_MANY, 'slides')
participant_tree.sample.portion.slide.annotation.corr = (ONE_TO_MANY, 'annotations')
participant_tree.tissue_source_site.corr = (ONE_TO_ONE, 'tissue_source_site')
participant_tree.file.corr = (ONE_TO_MANY, 'files')

participant_traversal = Dict()
participant_traversal.file = [
    ('sample', 'file'),
    ('sample', 'file', 'file'),
    ('sample', 'aliquot', 'file'),
    ('sample', 'aliquot', 'file', 'file'),
    ('sample', 'portion', 'analyte', 'file'),
    ('sample', 'portion', 'analyte', 'file', 'file'),
    ('sample', 'portion', 'analyte', 'aliquot', 'file'),
    ('sample', 'portion', 'analyte', 'aliquot', 'file', 'file'),
]

# Annotation hierarchy
annotation_tree = Dict()
annotation_tree.project.corr = (ONE_TO_ONE, 'project')
annotation_tree.project.program.corr = (ONE_TO_ONE, 'program')
annotation_tree.item.corr = (ONE_TO_ONE, 'item')

annotation_traversal = Dict()

# Project hierarchy
project_tree = Dict()
project_tree.program = (ONE_TO_ONE, 'program')
