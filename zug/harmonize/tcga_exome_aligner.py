import os

from sqlalchemy import func

from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    FileMemberOfExperimentalStrategy,
    FileDataFromAliquot,
)


class TCGAExomeAligner(object):

    def __init__(self, graph=None):
        if graph:
            self.graph = graph
        else:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
            )
        self.workdir = os.environ["ALIGNMENT_WORKDIR"]
        self.docker_image_id = os.environ["DOCKER_IMAGE_ID"]
        self.log = get_logger("tcga_exome_alignmer")

    def find_exome_to_align(self):
        """The strategy is as follows:

        1) Make a subquery for all TCGA exomes
        2) Select at random a single aliquot which is the source of a file in that subquery
        3) Select the most recently updated file of the aliquot
        """
        tcga_exome_bam_ids = self.graph.nodes(File.node_id)\
                                       .sysan(source="tcga_cghub")\
                                       .join(FileMemberOfExperimentalStrategy)\
                                       .join(ExperimentalStrategy)\
                                       .filter(ExperimentalStrategy.name.astext == "WXS")\
                                       .subquery()

        aliquot = self.graph.nodes(Aliquot)\
                            .join(FileDataFromAliquot)\
                            .join(File)\
                            .filter(File.node_id.in_(tcga_exome_bam_ids))\
                            .order_by(func.random())\
                            .first()
        self.log.info("Selected aliquot %s to work on, files: %s", aliquot)
        sorted_files = sorted([f for f in aliquot.files],
                              key=lambda f: f.sysan["cghub_last_modified"])
        self.log.info("Aliquot has %s files", len(sorted_files))
        file_to_work_on = sorted_files[0]
        self.log.info("Choosing file %s to align", file_to_work_on)
        return file_to_work_on

    def align_file(file):
