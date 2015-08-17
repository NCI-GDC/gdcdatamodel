import os
import re
import hashlib
import tempfile
import time
import shutil
from urlparse import urlparse
from cStringIO import StringIO

from sqlalchemy import func, desc, BigInteger
from sqlalchemy.pool import NullPool
import docker
from boto.s3.connection import OrdinaryCallingFormat
from requests.exceptions import ReadTimeout

# buffer 10 MB in memory at once
from boto.s3.key import Key
Key.BufferSize = 10 * 1024 * 1024


from psqlgraph import PsqlGraphDriver
from cdisutils import md5sum
from cdisutils.log import get_logger
from cdisutils.net import BotoManager, url_for_boto_key
from signpostclient import SignpostClient
from zug.consul_manager import ConsulManager
from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    Aliquot, File, ExperimentalStrategy,
    Platform, Center,
    FileDataFromAliquot, FileDataFromFile
)

from zug.harmonize.abstract_harmonizer import AbstractHarmonizer


class TCGAExomeAligner(AbstractHarmonizer):

    @property
    def name(self):
        return "tcga_exome_aligner"

    def get_config(self, kwargs):
        if os.environ.get("ALIGNMENT_SIZE_LIMIT"):
            size_limit = int(os.environ["ALIGNMENT_SIZE_LIMIT"])
        else:
            size_limit = None
        return {
            "output_buckets": {
                "bam": os.environ["BAM_S3_BUCKET"],
                "bai": os.environ["BAM_S3_BUCKET"],
                "log": os.environ["LOGS_S3_BUCKET"],
                "db": os.environ["LOGS_S3_BUCKET"],
            },
            # all paths are workdir-relative. this is convenient
            # because we can absolutize-them with respect to the
            # container or the host.
            "paths": {
                "reference": os.environ.get("ALIGNMENT_REFERENCE",
                                            "reference/GRCh38.d1.vd1.fa"),
                "intervals_dir": os.environ.get("ALIGNMENT_INTERVAL_DIR",
                                                "intervals/"),
                "libraryname_json": os.environ.get("ALIGNMENT_LIBRARYNAME_JSON",
                                                   "intervals/bam_libraryname_capturekey.json"),
                "intervalname_json": os.environ.get("ALIGNMENT_INTERVALNAME_JSON",
                                                    "intervals/bait_target_key_interval.json"),
            },
            "size_limit": size_limit,
            "cores": int(os.environ.get("ALIGNMENT_CORES", "8")),
            "force_input_id": kwargs.get("force_input_id"),
        }

    @property
    def valid_extra_kwargs(self):
        return ["force_input_id"]

    @property
    def input_schema(self):
        """The TCGA exome aligner has two inputs, a bam file and it's
        corresponding bai.
        """
        return {
            "bam": File,
            "bai": File,
        }

    @property
    def output_schema(self):
        """There are four outputs, a realigned bam, it's corresponding bai,
        the log of what happened, and a sqlite database with various
        statistics and metrics.

        """
        return {
            "bam": File,
            "bai": File,
            "log": File,
            "db": File,
        }

    def choose_bam_by_forced_id(self):
        input_bam = self.graph.nodes(File).ids(self.config["force_input_id"]).one()
        assert input_bam.sysan["source"] == "tcga_cghub"
        assert input_bam.file_name.endswith(".bam")
        assert input_bam.data_formats[0].name == "BAM"
        assert input_bam.experimental_strategies[0].name == "WXS"
        return input_bam

    def choose_bam_at_random(self):
        """This queries for a bam file that we can align at random,
        potentially filtering by size.

        """
        wxs = ExperimentalStrategy.name.astext == "WXS"
        broad = Center.short_name.astext == "BI"
        illumina = Platform.name.astext.contains("Illumina")
        currently_being_aligned = self.consul.list_locked_keys()
        # NOTE you would think that file_name filter would be
        # unnecessary but we have some TCGA exomes that end with
        # .bam_HOLD_QC_PENDING. I am not sure what to do with these so
        # for now I am ignoring them
        alignable_files = self.graph.nodes(File)\
                                    .props(state="live")\
                                    .sysan(source="tcga_cghub")\
                                    .join(FileDataFromAliquot)\
                                    .join(Aliquot)\
                                    .distinct(Aliquot.node_id.label("aliquot_id"))\
                                    .filter(File.experimental_strategies.any(wxs))\
                                    .filter(File.centers.any(broad))\
                                    .filter(File.platforms.any(illumina))\
                                    .filter(File.file_name.astext.endswith(".bam"))\
                                    .filter(~File.derived_files.any())\
                                    .filter(~File.node_id.in_(currently_being_aligned))\
                                    .order_by(Aliquot.node_id, desc(File._sysan["cghub_upload_date"].cast(BigInteger)))
        if self.config["size_limit"]:
            alignable_files = alignable_files.filter(
                File.file_size.cast(BigInteger) < self.config["size_limit"]
            )
        input_bam = alignable_files.from_self(File).order_by(func.random()).first()
        if not input_bam:
            raise NoMoreWorkException("We appear to have aligned all bam files")
        else:
            return input_bam

    def find_inputs(self):
        if self.config["force_input_id"]:
            input_bam = self.choose_bam_by_forced_id()
        else:
            input_bam = self.choose_bam_at_random()
        self.log.info("Choosing file %s to align", input_bam)
        self.log.info("Finding associated bai file")
        potential_bais = [f for f in input_bam.related_files
                          if f.file_name.endswith(".bai")]
        if not potential_bais:
            raise RuntimeError("No bai files associated with bam {}"
                               .format(input_bam))
        if len(potential_bais) > 1:
            raise RuntimeError("Multiple potential bais found for bam {}"
                               .format(potential_bais))
        input_bai = potential_bais[0]
        self.log.info("Found bai %s", input_bai)
        # we expunge from this session so we can merge into another
        # session later and load up it's classifiction nodes to
        # classify the newly produced bam
        self.graph.current_session().expunge(input_bam)
        self.graph.current_session().expunge(input_bai)
        # we return an id to lock and a dict of inputs
        return input_bam.node_id, {
            "bam": input_bam,
            "bai": input_bai,
        }

    def build_docker_cmd(self):
        return (
            "/home/ubuntu/.virtualenvs/p3/bin/python /home/ubuntu/pipelines/dnaseq/aln.py "
            "-r {reference_path} "
            "-b {bam_path} "
            "-u {file_id} "
            "-x "
            "-v {intervals_dir} "
            "-c {libraryname_json} "
            "-i {intervalname_json} "
            "-s {scratch_dir} "
            "-l {log_dir} "
            "-j {cores} "
            "-d"
        ).format(
            reference_path=self.container_abspath(self.config["reference"]),
            bam_path=self.container_abspath(self.input_paths["bam"]),
            file_id=self.inputs["bam"].node_id,
            intervals_dir=self.container_abspath(self.config["intervals_dir"]),
            libraryname_json=self.container_abspath(
                self.config["libraryname_json"]),
            intervalname_json=self.container_abspath(
                self.config["intervalname_json"]),
            scratch_dir=self.container_abspath(self.config["scratch_dir"]),
            cores=self.config["cores"],
            log_dir=self.container_abspath(self.config["scratch_dir"]),
        )

    @property
    def output_paths(self):
        return {
            "bam": self.host_abspath(
                self.config["scratch_dir"],
                "realn", "md",
                self.inputs["bam"].file_name
            ),
            "bai": self.host_abspath(
                self.config["scratch_dir"],
                "realn", "md",
                re.sub("\.bam$", ".bai", self.inputs["bam"].file_name)
            ),
            "log": self.host_abspath(
                self.config["scratch_dir"],
                "aln_" + self.inputs["bam"].node_id + ".log"
            ),
            "db": self.host_abspath(
                self.config["scratch_dir"],
                self.inputs["bam"].node_id + "_harmonize.db"
            ),
        }

    def upload_secondary_files(self):
        """
        Upload the log file and sqlite db to the relevant bucket
        """
        for key in ["log", "db"]:
            path = os.path.normpath(self.host_abspath(self.output_paths[key]))
            self.upload_file(
                path,
                self.config["output_buckets"][key],
                os.path.basename(path),
            )

    def handle_output(self):
        self.upload_secondary_files()
        output_nodes = {}
        for key in ["bam", "bai"]:
            name = self.inputs[key].file_name.replace(".bam", "_gdc_realn.bam")
            output_nodes[key] = self.upload_file_and_save_to_db(
                self.host_abspath(self.output_paths[key]),
                self.config["output_buckets"][key],
                name,
                self.inputs["bam"].acl
            )
        output_nodes["bam"].related_files = [output_nodes["bai"]]
        docker_tag = (self.docker_image["RepoTags"][0]
                      if self.docker_image["RepoTags"] else None)
        edge = FileDataFromFile(
            src=self.inputs["bam"],
            dst=output_nodes["bam"],
            system_annotations={
                "alignment_started": self.start_time,
                "alignment_finished": int(time.time()),
                # raw_docker as opposed to whatever we might use in
                # the future, e.g. CWL
                "alignment_method": "raw_docker",
                "alignment_docker_image_id": self.docker_image["Id"],
                "alignment_docker_image_tag": docker_tag,
                "alignment_docker_cmd": self.docker_cmd,
                "alignment_reference_name": os.path.basename(self.config["reference"]),
            }
        )
        with self.graph.session_scope() as session:
            # merge old bam file so we can get its classification
            session.add(self.inputs["bam"])
            # classify new bam file, same as the old bam file
            output_nodes["bam"].experimental_strategies = self.inputs["bam"].experimental_strategies
            output_nodes["bam"].data_formats =  self.inputs["bam"].data_formats
            output_nodes["bam"].data_subtypes = self.inputs["bam"].data_subtypes
            output_nodes["bam"].platforms = self.inputs["bam"].platforms
            # this line implicitly merges the new bam and new bai
            session.merge(edge)
