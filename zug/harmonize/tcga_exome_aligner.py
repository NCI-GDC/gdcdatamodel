import os

from zug.harmonize.workflow_registry import WorkflowRegistryClient


class TCGAExomeAligner(object):

    def __init__(self):
        self.workdir = os.environ["ALIGNMENT_WORKDIR"]
        # eventually this will take a url from the environemnt
        # and actually connect to a registry server
        self.registry_client = WorkflowRegistryClient()

    def find_exome_to_align(self):
        # TODO query for aliquot,
