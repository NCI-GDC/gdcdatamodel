import os

from sqlalchemy import func
import docker

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
        self.workdir = os.environ.get("ALIGNMENT_WORKDIR", "/mnt/alignment")
        self.container_workdir = "/alignment"
        self.docker_image_id = os.environ["DOCKER_IMAGE_ID"]
        # NOTE all of these path variables are relative to
        # self.workdir. this is useful since we can describe them to
        # the docker container relative to the path we mount the
        # workdir into the container at
        self.reference = os.environ.get("REFERENCE", "reference/GRCh38.d1.vd1.fa")
        self.scratch_dir = os.environ.get("SCRATCH_DIR", "scratch")
        self.cores = int(os.environ.get("ALIGNMENT_CORES" "8"))
        # TODO initialize this lazily
        self.docker = docker.Client()
        self.log = get_logger("tcga_exome_alignmer")

    def choose_bam_to_align(self):
        """The strategy is as follows:

        1) Make a subquery for all TCGA exomes
        2) Select at random a single aliquot which is the source of a file in that subquery
        3) Select the most recently updated file of the aliquot

        We then set self.input_bam to that file.
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
        self.input_bam = sorted_files[0]
        self.log.info("Choosing file %s to align", self.input_bam)

    def download_file_to_tmpdir(self):
        """TODO: need to decide how to do this. One interesting possibility
        is to just download via the API. This is simpler in the sense
        that it abstracts the object stores away so that this code
        does not need access to them. However, this would configuring
        this code with credentials to the API (and making sure they
        didn't get rotated, etc.), and would potentially interfere
        with the API for testing users (altough we'd prefer to
        discover scalability issues now rather than later I
        suppose). It would also probably be faster to hit the object
        stores directly.

        Regardless, this will need to set self.input_bam_path (which
        should be in self.scratch_dir)
        """
        raise NotImplementedError()

    def host_abspath(self, relative_path):
        return os.path.join(self.workdir, relative_path)

    def container_abspath(self, relative_path):
        return os.path.join(self.container_workdir, relative_path)

    def run_docker_alignment(self):
        filtered_images = [i for i in self.docker.images()
                           if i["Id"] == self.docker_image_id]
        if not filtered_images:
            raise RuntimeError("No docker image with id {} found!".format(self.docker_image_id))
        image = filtered_images[1]
        cmd = ("/home/ubuntu/.virtualenvs/p3/bin/python /home/ubuntu/apipe/aln.py "
               "-r {reference_path} "
               "-b {bam_path} "
               "-u {file_id} "
               "-s {scratch_dir} "
               "-t {cores} "
               "-l {log_dir} "
               "-d").format(
                   reference_path=self.container_abspath(self.reference),
                   bam_path=self.container_abspath(self.input_bam_path),
                   file_id=self.input_bam.node_id,
                   scratch_dir=self.container_abspath(self.scratch_dir),
                   cores=self.cores,
                   log_dir=self.container_abspath(self.scratch_dir),
               )
        self.log.info("Creating docker container")
        self.log.info("Docker image id: %s", image["Id"])
        self.log.info("Docker command: %s", cmd)
        self.log.info("Mapping host volume %s to container volume %s",
                      self.workdir, self.container_workdir)
        host_config = docker.utils.create_host_config(binds={
            self.workdir: {
                "bind": self.container_workdir,
                "ro": False,
            },
        })
        container = self.docker.create_container(
            image=image["Id"],
            command=cmd,
            host_config=host_config,
        )
        self.log.info("Starting docker container")
        self.docker.start(container)
        self.log.info("Container run finished, removing")
        self.docker.remove_container(container, v=True)

    def upload_output(self):
        """
        1) Locate output bam
        2) Use config to decide where to put it
        3) Upload (this will have to use boto)
        """
        raise NotImplementedError()

    def align(self):
        self.download_file_to_tmpdir()
        self.run_docker_alignment()
        self.upload_output()
        self.create_output_node()
