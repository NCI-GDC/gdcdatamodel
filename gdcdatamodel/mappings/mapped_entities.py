from addict import Dict

# File hierarchy
file_tree = Dict()
file_tree.annotation = {}
file_tree.archive = {}
file_tree.center = {}
file_tree.data_format = {}
file_tree.data_subtype.data_type = {}
file_tree.experimental_strategy = {}
file_tree.platform = {}
file_tree.tag = {}

# Participant hierarchy
participant_tree = Dict()
participant_tree.clinical = {}
participant_tree.project.program = {}
participant_tree.sample.aliquot.annotation = {}
participant_tree.sample.annotation = {}
participant_tree.sample.portion.analyte.aliquot.annotation = {}
participant_tree.sample.portion.analyte.aliquot.center = {}
participant_tree.sample.portion.analyte.annotation = {}
participant_tree.sample.portion.annotation = {}
participant_tree.sample.portion.center = {}
participant_tree.sample.portion.slide.annotation = {}
participant_tree.tissue_source_site = {}

# Annotation hierarchy
annotation_tree = Dict()
annotation_tree.aliquot = {}
annotation_tree.analyte = {}
annotation_tree.participant = {}
annotation_tree.portion = {}
annotation_tree.project.program = {}
annotation_tree.sample = {}
annotation_tree.slide = {}

# Project hierarchy
project_tree = Dict()
project_tree.program = {}
