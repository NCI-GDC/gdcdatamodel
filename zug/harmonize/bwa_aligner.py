import os
import re
import time
from datadog import statsd
import socket
from zug.binutils import NoMoreWorkException
from gdcdatamodel.models import (
    File, FileDataFromFile
)

from zug.harmonize.abstract_harmonizer import AbstractHarmonizer
from zug.harmonize.queries import SORT_ORDER

import gzip


# these next two are from http://stackoverflow.com/a/260433/1663558

def reversed_lines(file):
    "Generate the lines of file in reverse order."
    part = ''
    for block in reversed_blocks(file):
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]


def reversed_blocks(file, blocksize=4096):
    "Generate blocks of file's contents in reverse order."
    file.seek(0, os.SEEK_END)
    here = file.tell()
    while 0 < here:
        delta = min(blocksize, here)
        here -= delta
        file.seek(here, os.SEEK_SET)
        yield file.read(delta)


def parse_last_step(logfile):
    for line in reversed_lines(logfile):
        if "running step" in line and "completed" not in line:
            match = re.match(".*running step (.*) of: (.*)", line)
            step, name = match.group(1), match.group(2)
            step = step.strip()
            step = step.strip("`")  # some step names are wrapped in backticks
            name = name.strip()
            return step, name
    raise RuntimeError("Couldn't find last step in {}".format(logfile))


def has_fixmate_failure(logs):
    return "FixMateInformation" in logs


def has_markdups_failure(logs):
    return "MarkDuplicatesWithMateCigar" in logs


def gzip_compress(input_path):
    """Given a path, gzip compress it at the same path but with .gz on
    the end and return the path of the gzip compressed file
    """
    output_path = input_path + ".gz"
    with open(input_path, "rb") as input, gzip.open(output_path, "wb") as output:
        output.writelines(input)
    return output_path


class BWAAligner(AbstractHarmonizer):

    def get_config(self, kwargs):
        if os.environ.get("ALIGNMENT_SIZE_LIMIT"):
            size_limit = int(os.environ["ALIGNMENT_SIZE_LIMIT"])
        else:
            size_limit = None
        if os.environ.get("ALIGNMENT_SIZE_MIN"):
            size_min = int(os.environ["ALIGNMENT_SIZE_MIN"])
        else:
            size_min = None
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
            "size_min": size_min,
            "cores": int(os.environ.get("ALIGNMENT_CORES", "8")),
            "force_input_id": kwargs.get("force_input_id"),
            "center_limit": os.environ.get("ALIGNMENT_CENTER_LIMIT")
        }

    @property
    def docker_log_flag_funcs(self):
        return {
            "fixmate_failure": has_fixmate_failure,
            "markdups_failure": has_markdups_failure
        }

    def docker_failure_cleanup(self, logs):
        if self.docker_log_flags["fixmate_failure"]:
            with self.graph.session_scope() as session:
                session.add(self.inputs["bam"])
                self.inputs["bam"].sysan["alignment_fixmate_failure"] = True
        if self.docker_log_flags["markdups_failure"]:
            with self.graph.session_scope() as session:
                session.add(self.inputs["bam"])
                self.inputs["bam"].sysan["alignment_markdups_failure"] = True
        try:
            # attempt to parse last failing step from logs
            log_path = self.host_abspath(
                self.config["scratch_dir"],
                "aln_" + self.inputs["bam"].node_id + ".log"
            )
            step, name = parse_last_step(open(log_path))
            with self.graph.session_scope() as session:
                session.add(self.inputs["bam"])
                self.inputs["bam"].sysan["alignment_last_docker_error_step"] = step
                self.inputs["bam"].sysan["alignment_last_docker_error_filename"] = name
        except:
            self.log.exception("caught exception while parsing log file")
        return super(BWAAligner, self).docker_failure_cleanup(logs)

    @property
    def valid_extra_kwargs(self):
        return ["force_input_id"]

    @property
    def input_schema(self):
        """The BWA aligner has two inputs, a bam file and it's
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

    def choose_bam_at_random(self):
        """This queries for a bam file that we can align at random,
        potentially filtering by size.

        """
        total_bams = self.bam_files.count()
        aligned = self.bam_files.filter(File.derived_files.any()).count()
        self.log.info("Aligned %s out of %s files", aligned, total_bams)
        input_bam = self.alignable_files.from_self(File).order_by(*SORT_ORDER).first()
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
        cmd = (
            "/home/ubuntu/.virtualenvs/p3/bin/python /home/ubuntu/pipelines/dnaseq/aln.py "
            "-r {reference_path} "
            "-b {bam_path} "
            "-u {file_id} "
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
        # all these conditionals should really be done by inheritance
        # but this is easier for now; forgive me gods of object
        # orientation
        if "exome" in self.name:
            cmd += " -x"
            if self.name == "target_exome_aligner":
                # session scope is needed to load the center
                with self.graph.session_scope() as session:
                    session.add(self.inputs["bam"])
                    center_name = self.inputs["bam"].sysan["cghub_center_name"]
                # this specifies that we are doing target
                # alignment and what the center is
                cmd += " -g -q {center_name}".format(
                    center_name=center_name
                )
        return cmd

    @property
    def output_paths(self):
        bam_path = None
        # important that this list is in our order of preference.  if
        # markduplicates has been run we want that, if fixmate has
        # been run we want that, etc.
        for possible_dir in ["md", "fixmate", "reheader"]:
            possible_path = self.host_abspath(
                self.config["scratch_dir"],
                "realn", possible_dir,
                self.inputs["bam"].file_name
            )
            if os.path.exists(possible_path):
                bam_path = possible_path
                break
        if not bam_path:
            raise RuntimeError("Can't find output bam file")
        return {
            "bam": bam_path,
            "bai": re.sub("\.bam$", ".bai", bam_path),
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
        # upload db
        db_path = os.path.normpath(self.host_abspath(self.output_paths["db"]))
        self.upload_file(
            db_path,
            self.config["output_buckets"]["db"],
            os.path.basename(db_path),
        )
        # compress and upload log
        log_path = os.path.normpath(self.host_abspath(self.output_paths["log"]))
        self.log.info("Compressing log file at %s", log_path)
        compressed_log_path = gzip_compress(log_path)
        self.log.info("Compressed log file to %s", compressed_log_path)
        self.upload_file(
            compressed_log_path,
            self.config["output_buckets"]["log"],
            os.path.basename(compressed_log_path),
        )


    def submit_metrics(self):
        """
        Submit metrics to datadog
        """
        self.log.info("Submitting metrics")
        took = int(time.time()) - self.start_time
        input_id = self.inputs["bam"].node_id

        tags=["alignment_type:{}".format(self.name),
              "alignment_host:{}".format(socket.gethostname())]
        statsd.event(
            "{} aligned".format(input_id),
            "successfully aligned {} in {} minutes".format(input_id, took / 60),
            source_type_name="harmonization",
            alert_type="success",
            tags=tags
        )
        with self.graph.session_scope():
            total = self.bam_files.count()
            done = self.bam_files.filter(File.derived_files.any()).count()
        self.log.info("%s bams aligned out of %s", done, total)
        statsd.gauge('harmonization.completed_bams',
                     done,
                     tags=tags)
        statsd.gauge('harmonization.total_bams',
                     total,
                     tags=tags)
        statsd.histogram('harmonization.seconds',
                         took,
                         tags=tags)
        statsd.histogram('harmonization.seconds_per_byte',
                         float(took) / self.inputs["bam"].file_size,
                         tags=tags)

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
                "alignment_hostname": socket.gethostname(),
                "alignment_host_openstack_uuid": self.openstack_uuid,
                "alignment_last_step": self.output_paths["bam"].split("/")[-2],
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
