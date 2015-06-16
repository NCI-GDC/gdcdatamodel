import os
import hashlib
import tempfile
from urlparse import urlparse

from sqlalchemy import func
import docker
from boto.s3.connection import OrdinaryCallingFormat

from psqlgraph import PsqlGraphDriver
from cdisutils.log import get_logger
from cdisutils.net import BotoManager
from signpostclient import SignpostClient
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    FileMemberOfExperimentalStrategy,
    FileDataFromAliquot,
)


def first_s3_url(doc):
    for url in doc.urls:
        parsed = urlparse(url)
        if parsed.scheme == "s3":
            return url
    raise RuntimeError("File {} does not have s3 urls.".format(doc.id))


def boto_config():
    """
    Prepare a configuration dict suitable to pass to
    cdisutils.net.BotoManager from the process environment.  This is a
    bit hardcode-y and I would prefer this to get passed in via the
    process environment somehow but it's a complicated nested structure
    that involves python variables and stuff and the alternative is trying to
    like import a python file so I'm just gonna do it here

    """
    return {
        "cleversafe.service.consul": {
            "aws_access_key_id": os.environ["CLEV_ACCESS_KEY"],
            "aws_secret_access_key": os.environ["CLEV_SECRET_KEY"],
            "is_secure": False,
            "calling_format": OrdinaryCallingFormat()
        },
        "ceph.service.consul": {
            "aws_access_key_id": os.environ["CEPH_ACCESS_KEY"],
            "aws_secret_access_key": os.environ["CEPH_SECRET_KEY"],
            "is_secure": False,
            "calling_format": OrdinaryCallingFormat()
        },
    }


class TCGAExomeAligner(object):

    def __init__(self, graph=None, s3=None,
                 signpost=None):
        if graph:
            self.graph = graph
        else:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
            )
        if s3:
            self.s3 = s3
        else:
            self.s3 = BotoManager(boto_config())
        if signpost:
            self.signpost = signpost
        else:
            self.signpost = SignpostClient(os.environ["SIGNPOST_URL"])
        # TODO make more of this stuff passable by kwargs
        self.workdir = os.environ.get("ALIGNMENT_WORKDIR", "/mnt/alignment")
        self.container_workdir = "/alignment"
        self.docker_image_id = os.environ["DOCKER_IMAGE_ID"]
        # NOTE all of these path variables are relative to
        # self.workdir. this is useful since we can describe them to
        # the docker container relative to the path we mount the
        # workdir into the container at
        self.reference = os.environ.get("REFERENCE", "reference/GRCh38.d1.vd1.fa")
        scratch_dir = tempfile.mkdtemp(prefix="scratch", dir=self.workdir)
        # make it relative to workdir
        self.scratch_dir = os.path.relpath(scratch_dir, start=self.workdir)
        self.cores = int(os.environ.get("ALIGNMENT_CORES", "8"))
        # TODO initialize this lazily
        self.docker = docker.Client(
            **docker.utils.kwargs_from_env(assert_hostname=False)
        )
        self.log = get_logger("tcga_exome_aligner")

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
        self.log.info("Selected aliquot %s to work on", aliquot)
        sorted_files = sorted([f for f in aliquot.files],
                              key=lambda f: f.sysan["cghub_last_modified"])
        self.log.info("Aliquot has %s files", len(sorted_files))
        # TODO LOCK IT (probably in consul) so no one else gets it
        self.input_bam = sorted_files[0]
        self.log.info("Choosing file %s to align", self.input_bam)

    def download_input_bam(self):
        """
        This hist the object stores directly, although we should consider
        hitting the API instead in the future.
        """
        self.log.info("Querying signpost for file urls")
        doc = self.signpost.get(self.input_bam.node_id)
        url = first_s3_url(doc)
        self.log.info("Getting key for url %s", url)
        key = self.s3.get_url(url)
        path = os.path.join(self.workdir, self.scratch_dir,
                            self.input_bam.file_name)
        md5 = hashlib.md5()
        with open(path, "w") as f:
            self.log.info("Saving file from s3 to %s", path)
            key.BufferSize = 10 * 1024 * 1024
            for chunk in key:
                md5.update(chunk)
                f.write(chunk)
        md5sum = md5.hexdigest()
        if md5sum != self.input_bam.md5sum:
            raise RuntimeError("Downloaded md5sum {} != "
                               "database md5sum {}".format(md5sum, file.md5sum))
        else:
            self.input_bam_path = os.path.join(self.scratch_dir,
                                               self.input_bam.file_name)

    def host_abspath(self, relative_path):
        return os.path.join(self.workdir, relative_path)

    def container_abspath(self, relative_path):
        return os.path.join(self.container_workdir, relative_path)

    def build_docker_cmd(self):
        return (
            "/home/ubuntu/.virtualenvs/p3/bin/python /home/ubuntu/apipe/aln.py "
            "-r {reference_path} "
            "-b {bam_path} "
            "-u {file_id} "
            "-s {scratch_dir} "
            "-t {cores} "
            "-l {log_dir} "
            "-d"
        ).format(
            reference_path=self.container_abspath(self.reference),
            bam_path=self.container_abspath(self.input_bam_path),
            file_id=self.input_bam.node_id,
            scratch_dir=self.container_abspath(self.scratch_dir),
            cores=self.cores,
            log_dir=self.container_abspath(self.scratch_dir),
        )

    def run_docker_alignment(self):
        filtered_images = [i for i in self.docker.images()
                           if i["Id"] == self.docker_image_id]
        if not filtered_images:
            raise RuntimeError("No docker image with id {} found!".format(self.docker_image_id))
        image = filtered_images[0]
        self.log.info("Creating docker container")
        self.log.info("Docker image id: %s", image["Id"])
        self.docker_cmd = self.build_docker_cmd()
        self.log.info("Mapping host volume %s to container volume %s",
                      self.workdir, self.container_workdir)
        host_config = docker.utils.create_host_config(binds={
            self.workdir: {
                "bind": self.container_workdir,
                "ro": False,
            },
        })
        self.log.info("Docker command: %s", self.docker_cmd)
        container = self.docker.create_container(
            image=image["Id"],
            command=self.docker_cmd,
            host_config=host_config,
        )
        self.log.info("Starting docker container and waiting for it to complete")
        self.docker.start(container)
        for log in self.docker.logs(container, stream=True, stdout=True, stderr=True):
            self.log.info(log)  # TODO maybe something better
        retcode = self.docker.wait(container)
        if retcode != 0:
            raise RuntimeError("Docker container failed with exit code {}".format(retcode))
        self.log.info("Container run finished successfully, removing")
        self.docker.remove_container(container, v=True)

    def upload_output(self):
        """
        1) Locate output bam
        2) Use config to decide where to put it
        3) Upload (this will have to use boto)
        """
        raise NotImplementedError()

    def align(self):
        # TODO more fine grained transactions?
        with self.graph.session_scope():
            self.choose_bam_to_align()
            self.download_input_bam()
            self.run_docker_alignment()
            # self.upload_output()
            # self.create_output_node()
