import os
import hashlib
import tempfile
import time
import shutil
from urlparse import urlparse
from cStringIO import StringIO

from sqlalchemy import func
import docker
from boto.s3.connection import OrdinaryCallingFormat

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
    FileDataFromAliquot, FileDataFromFile
)


def read_in_chunks(file_object, chunk_size=1024*1024*1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


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
                 signpost=None,
                 consul_prefix="tcga_exome_aligner"):
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
        upload_host = os.environ["UPLOAD_S3_HOST"]
        if upload_host not in self.s3.hosts:
            raise RuntimeError("Host {} not one of known hosts: {}".format(upload_host, self.s3.hosts))
        else:
            self.upload_host = upload_host
        # TODO verify that these buckets exist
        self.bam_bucket = os.environ["BAM_S3_BUCKET"]
        self.logs_bucket = os.environ["LOGS_S3_BUCKET"]
        self.workdir = os.environ.get("ALIGNMENT_WORKDIR", "/mnt/alignment")
        self.container_workdir = "/alignment"
        self.docker_image_id = os.environ["DOCKER_IMAGE_ID"]
        # NOTE all of these path variables are relative to
        # self.workdir. this is useful since we can describe them to
        # the docker container relative to the path we mount the
        # workdir into the container at
        self.reference = os.path.normpath(
            os.environ.get("REFERENCE", "reference/GRCh38.d1.vd1.fa")
        )
        scratch_dir = tempfile.mkdtemp(prefix="scratch", dir=self.workdir)
        # make it relative to workdir
        self.scratch_dir = os.path.relpath(scratch_dir, start=self.workdir)
        self.cores = int(os.environ.get("ALIGNMENT_CORES", "8"))
        self.init_docker()
        self.consul = ConsulManager(prefix=consul_prefix)
        self.consul.start_consul_session()
        self.start_time = int(time.time())
        self.log = get_logger("tcga_exome_aligner")

    def init_docker(self):
        kwargs = docker.utils.kwargs_from_env(assert_hostname=False)
        # let's be very conservative here since I'm not sure how long
        # the pipeline might block without any output
        kwargs["timeout"] = 3*60*60
        self.docker = docker.Client(**kwargs)

    def choose_bam_to_align(self):
        """The strategy is as follows:

        1) Make a subquery for all TCGA exomes
        2) Select at random a single aliquot which is the
        source of a file in that subquery
        3) Select the most recently updated file of the
        aliquot that doesn't already have a FileDataFromFile
        edge

        We then set self.input_bam to that file.
        """
        # NOTE you would think that second .filter would be unnecessary, but
        # we have some TCGA exomes that end with
        # .bam_HOLD_QC_PENDING. I am not sure what to do with these so
        # for now I am ignoring them
        tcga_exome_bam_ids = self.graph.nodes(File.node_id)\
                                       .sysan(source="tcga_cghub")\
                                       .filter(File.experimental_strategies.any(ExperimentalStrategy.name.astext == "WXS"))\
                                       .filter(File.file_name.astext.endswith(".bam"))\
                                       .filter(~File.derived_files.any())\
                                       .subquery()
        aliquot = self.graph.nodes(Aliquot)\
                            .join(FileDataFromAliquot)\
                            .join(File)\
                            .filter(File.node_id.in_(tcga_exome_bam_ids))\
                            .order_by(func.random())\
                            .first()
        if not aliquot:
            raise NoMoreWorkException("No aliquots with unaligned files found")
        self.log.info("Selected aliquot %s to work on", aliquot)
        sorted_files = sorted([f for f in aliquot.files],
                              key=lambda f: f.sysan["cghub_upload_date"],
                              reverse=True)
        self.log.info("Aliquot has %s files", len(sorted_files))
        input_bam = sorted_files[0]
        locked = self.consul.get_consul_lock(input_bam.node_id)
        if locked:
            self.log.info("locked consul key: %s", self.consul.consul_key)
            self.input_bam = input_bam
        else:
            raise RuntimeError("Couldn't lock consul key {}"
                               .format(self.consul.consul_key))
        self.log.info("Choosing file %s to align", self.input_bam)
        self.log.info("Finding associated bai file")
        potential_bais = [f for f in self.input_bam.related_files if f.file_name.endswith(".bai")]
        if not potential_bais:
            raise RuntimeError("No bai files associated with bam {}".format(self.input_bam))
        if len(potential_bais) > 1:
            raise RuntimeError("Multiple potential bais found for bam {}".format(potential_bais))
        self.input_bai = potential_bais[0]
        self.log.info("Found bai %s", self.input_bai)

    def download_file(self, file):
        """
        Download a file node from s3, returning it's workdir relative path.
        """
        self.log.info("Downloading file %s, size %s", file, file.file_size)
        self.log.info("Querying signpost for file urls")
        doc = self.signpost.get(file.node_id)
        url = first_s3_url(doc)
        self.log.info("Getting key for url %s", url)
        key = self.s3.get_url(url)
        workdir_relative_path = os.path.join(self.scratch_dir, file.file_name)
        abs_path = os.path.join(self.workdir, workdir_relative_path)
        md5 = hashlib.md5()
        with open(abs_path, "w") as f:
            self.log.info("Saving file from s3 to %s", abs_path)
            for chunk in key:
                md5.update(chunk)
                f.write(chunk)
        digest = md5.hexdigest()
        if digest != file.md5sum:
            raise RuntimeError("Downloaded md5sum {} != "
                               "database md5sum {}".format(digest, file.md5sum))
        else:
            return workdir_relative_path

    def download_inputs(self):
        """
        This hits the object stores directly, although we should consider
        hitting the API instead in the future.
        """
        self.input_bam_path = self.download_file(self.input_bam)
        self.input_bai_path = self.download_file(self.input_bai)

    def host_abspath(self, *relative_path):
        return os.path.join(self.workdir, os.path.join(*relative_path))

    def container_abspath(self, *relative_path):
        return os.path.join(self.container_workdir, os.path.join(*relative_path))

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
        self.docker_image = filtered_images[0]
        self.log.info("Creating docker container")
        self.log.info("Docker image id: %s", self.docker_image["Id"])
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
            image=self.docker_image["Id"],
            command=self.docker_cmd,
            host_config=host_config,
            user="root",
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

    @property
    def output_bam_path(self):
        return self.host_abspath(
            self.scratch_dir,
            "realn", "bwa_mem_pe", "md",
            self.input_bam.file_name
        )

    @property
    def output_bai_path(self):
        return self.host_abspath(
            self.scratch_dir,
            "realn", "bwa_mem_pe", "md",
            self.input_bam.file_name+".bai"
        )

    @property
    def output_logs_path(self):
        return self.host_abspath(
            self.scratch_dir,
            "aln_" + self.input_bam.node_id + ".log"
        )

    @property
    def output_db_path(self):
        return self.host_abspath(
            self.scratch_dir,
            self.input_bam.node_id + "_harmonize.db"
        )

    @property
    def output_paths(self):
        return [
            self.output_bam_path,
            self.output_logs_path,
            self.output_db_path,
        ]

    def check_output_paths(self):
        self.log.info("Checking output paths")
        for path in self.output_paths:
            self.log.info("Checking for existance %s", path)
            if not os.path.exists(path):
                raise RuntimeError("Output path does not exist: {}".format(path))

    def upload_file(self, abs_path, bucket, name, verify=True):
        """Upload the file at abs_path to bucket with key named name. Then
        download again, verify md5sum and return it.
        """
        self.log.info("Uploading %s to bucket %s from path %s",
                      name, bucket, abs_path)
        self.log.info("Getting bucket")
        bucket = self.s3[self.upload_host].get_bucket(bucket)
        self.log.info("Initiating multipart upload")
        mp = bucket.initiate_multipart_upload(name)
        md5 = hashlib.md5()
        with open(abs_path) as f:
            num_parts = 0
            for i, chunk in enumerate(read_in_chunks(f), start=1):
                self.log.info("Uploading chunk %s", i)
                md5.update(chunk)
                sio = StringIO(chunk)
                tries = 0
                while tries < 30:
                    tries += 1
                    try:
                        mp.upload_part_from_file(sio, i)
                        break
                    except KeyboardInterrupt:
                        raise
                    except:
                        self.log.exception(
                            "caught exception while uploading part %s, try %s "
                            "sleeping for 2 seconds and retrying", i, tries
                        )
                        time.sleep(2)
                num_parts += 1
                self.log.info("Reading next chunk from disk")
        self.log.info("Completing multipart upload")
        parts_on_s3 = len(mp.get_all_parts())
        if num_parts != parts_on_s3:
            raise RuntimeError("Number of parts sent %s "
                               "does not equal number of parts on s3 %s",
                               num_parts, parts_on_s3)
        completed_mp = mp.complete_upload()
        key = bucket.get_key(completed_mp.key_name)
        assert key.name == name
        uploaded_md5 = md5.hexdigest()
        self.log.info("Uploaded md5 is %s", uploaded_md5)
        if verify:
            disk_size = os.path.getsize(abs_path)
            s3_size = int(key.size)
            if disk_size != s3_size:
                raise RuntimeError("Size on disk {} does not "
                                   "match size on s3 {}"
                                   .format(disk_size, s3_size))
            self.log.info("md5ing from s3 to verify")
            md5_on_s3 = md5sum(key)
            if uploaded_md5 != md5_on_s3:
                raise RuntimeError("checksums do not match: "
                                   "uploaded {}, s3 has {}"
                                   .format(uploaded_md5, md5_on_s3))
            else:
                self.log.info("md5s match: %s", uploaded_md5)
        else:
            self.log.info("skipping md5 verification")
        return key, uploaded_md5

    def upload_secondary_files(self):
        """
        Upload the log file and sqlite db to the relevant bucket
        """
        # TODO put the normpath inside the abspath functions?
        logs_path = os.path.normpath(self.host_abspath(self.output_logs_path))
        self.upload_file(
            logs_path,
            self.logs_bucket,
            os.path.basename(logs_path),
            verify=False,
        )
        db_path = os.path.normpath(self.host_abspath(self.output_db_path))
        self.upload_file(
            db_path,
            self.logs_bucket,
            os.path.basename(db_path),
            verify=False,
        )

    def upload_file_and_save_to_db(self, abs_path, bucket, file_name):
        """
        Upload a file and save it in the db/signpost such that it's
        downloadable. The s3 key name is computed as {node_id}/{file_name}
        """
        self.log.info("Allocating id from signpost")
        doc = self.signpost.create()
        self.log.info("New id: %s", doc.did)
        s3_key_name = "/".join([
            doc.did,
            file_name
        ])
        self.log.info("Uploading file with s3 key %s to bucket %s",
                      s3_key_name, bucket)
        s3_key, md5 = self.upload_file(
            abs_path,
            bucket,
            s3_key_name
        )
        url = url_for_boto_key(s3_key)
        self.log.info("Patching signpost with url %s", url)
        doc.urls = [url]
        doc.patch()
        file_node = File(
            node_id=doc.did,
            file_name=file_name,
            md5sum=md5,
            file_size=int(s3_key.size),
            # TODO ????? should this be live? idk
            state="uploaded",
            state_comment=None,
            submitter_id=None,
        )
        file_node.system_annotations = {
            "source": "tcga_exome_alignment",
            # TODO anything else here?
        }
        self.log.info("File node: %s", file_node)
        return file_node

    def upload_output(self):
        self.check_output_paths()
        self.upload_secondary_files()
        bam_name = self.input_bam.file_name.replace(".bam", "_gdc_realn.bam")
        new_bam_node = self.upload_file_and_save_to_db(
            self.host_abspath(self.output_bam_path),
            self.bam_bucket,
            bam_name
        )
        bai_name = self.input_bai.file_name.replace(".bam", "_gdc_realn.bam")
        new_bai_node = self.upload_file_and_save_to_db(
            self.host_abspath(self.output_bai_path),
            self.bam_bucket,
            bai_name
        )
        new_bam_node.related_files = [new_bai_node]
        docker_tag = (self.docker_image["RepoTags"][0]
                      if self.docker_image["RepoTags"] else None)
        edge = FileDataFromFile(
            src=self.input_bam,
            dst=new_bam_node,
            system_annotations={
                "alignment_started": self.start_time,
                "alignment_finished": int(time.time()),
                # raw_docker as opposed to whatever we might use in
                # the future, e.g. CWL
                "alignment_method": "raw_docker",
                "alignment_docker_image_id": self.docker_image["Id"],
                "alignment_docker_image_tag": docker_tag,
                "alignment_docker_cmd": self.docker_cmd,
                "alignment_reference_name": os.path.basename(self.reference),
            }
        )
        # this line implicitly merges the new bam and new bai
        self.graph.current_session().merge(edge)
        # classify new bam file, same as the old bam file
        new_bam_node.experimental_strategies = self.input_bam.experimental_strategies
        new_bam_node.data_formats = self.input_bam.data_formats
        new_bam_node.data_subtypes = self.input_bam.data_subtypes
        new_bam_node.platforms = self.input_bam.platforms

    def cleanup(self):
        scratch_abspath = self.host_abspath(self.scratch_dir)
        self.log.info("Removing scatch dir %s", scratch_abspath)
        shutil.rmtree(scratch_abspath)

    def align(self):
        try:
            # TODO more fine grained transactions?
            with self.graph.session_scope():
                self.choose_bam_to_align()
                self.download_inputs()
                self.run_docker_alignment()
                self.upload_output()
        finally:
            self.cleanup()
