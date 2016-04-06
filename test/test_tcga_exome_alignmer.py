import os
import sys
import tempfile
import random
import string
import subprocess
from contextlib import nested
import gzip
from cStringIO import StringIO
import socket

from mock import patch, Mock
from base import ZugTestBase, FakeS3Mixin, SignpostMixin, PreludeMixin

from zug.harmonize.abstract_harmonizer import DockerFailedException
from zug.harmonize.tcga_exome_aligner import TCGAExomeAligner
from zug.binutils import NoMoreWorkException
# TODO really need to find a better place for this
from zug.downloaders import md5sum_with_size
from cdisutils.net import BotoManager
from gdcdatamodel.models import (
    File, ExperimentalStrategy,
    DataFormat, Platform, Center,
    FileDataFromFile
)
from boto.s3.connection import OrdinaryCallingFormat

from base import TEST_DIR

FIXTURES_DIR = os.path.join(TEST_DIR, "fixtures", "alignment")
FAKE_LOGS_PATH = os.path.join(FIXTURES_DIR, "fake_log.txt")
FAKE_LOGS_CONTENT = open(FAKE_LOGS_PATH).read()


class FakeDockerClient(object):
    """
    Docker doesn't work on travis CI because the kernel version is to
    old, so we write this fake docker client which just execs all nthe
    commands you tell it to do on the host machine.
    """

    def __init__(self, *args, **kwargs):
        self.containers = {}
        self._fail = False
        self._logs = ["FAKE", "DOCKER", "LOGS"]

    def images(self):
        return [{
            u'Created': 1434123150,
            u'Id': u'6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6',
            u'Labels': {},
            u'ParentId': u'9fd3c8c9af32dddb1793ccb5f6535e12d735eacae16f8f8c4214f42f33fe3d29',
            u'RepoDigests': [],
            u'RepoTags': [u'ubuntu:14.04'],
            u'Size': 0,
            u'VirtualSize': 188310849
        }]

    def create_container(self, **kwargs):
        id = ''.join(random.choice(string.ascii_uppercase + string.digits)
                     for _ in range(10))
        cont = kwargs
        cont["Id"] = id
        self.containers[id] = cont
        return cont

    def start(self, cont, **kwargs):
        retcode = subprocess.call(self.containers[cont["Id"]]["command"],
                                  shell=True)
        if self._fail:
            self.containers[cont["Id"]]["retcode"] = 1
        else:
            self.containers[cont["Id"]]["retcode"] = retcode

    def logs(self, cont, **kwargs):
        return self._logs

    def wait(self, cont, **kwargs):
        return self.containers[cont["Id"]]["retcode"]

    def remove_container(self, cont, **kwargs):
        del self.containers[cont["Id"]]


def fake_build_docker_cmd(output_dir="md"):
    """
    Simulate the action of running the actual docker container.
    """
    def inner_fake_build_docker_cmd(self):
        assert self.config["cores"] > 0
        todo = [
            "set -e",
            # assert that input bam exists
            "test -e {bam_path}",
            "test -e {bai_path}",
            # create fake outputs
            "mkfile() {{ mkdir -p $( dirname $1) && touch $1; }}",
            "mkfile {output_bam_path}",
            "echo -n fake_output_bam > {output_bam_path}",
            "mkfile {output_bai_path}",
            "echo -n fake_output_bai > {output_bai_path}",
            "mkfile {output_log_path}",
            "cp {FAKE_LOGS_PATH} {output_log_path}",
            "mkfile {output_db_path}",
            "echo -n fake_db > {output_db_path}",
        ]
        # yes i know this is kind of gross but it works just work with me
        # here ok
        template = "bash -c '" + "; ".join(todo) + "'"
        if os.environ.get("TRAVIS"):
            # we have to use the host path on travis because we don't have
            # real docker there, so we use FakeDockerClient, which just
            # executes the command on the host, so we need host relative
            # paths as opposed to container relative paths
            get_path = self.host_abspath
        else:
            get_path = self.container_abspath
        output_bam_path = get_path(os.path.join(
            self.config["scratch_dir"], "realn", output_dir,
            self.inputs["bam"].file_name)
        )
        output_bai_path = output_bam_path.replace(".bam", ".bai")
        output_log_path = get_path(os.path.join(
            self.config["scratch_dir"], "aln_"+self.inputs["bam"].node_id+".log")
        )
        output_db_path = get_path(os.path.join(
            self.config["scratch_dir"], self.inputs["bam"].node_id+"_harmonize.db")
        )
        return template.format(
            reference_path=get_path(self.config["reference"]),
            bam_path=get_path(self.input_paths["bam"]),
            bai_path=get_path(self.input_paths["bam"]),
            output_bam_path=output_bam_path,
            output_bai_path=output_bai_path,
            output_log_path=output_log_path,
            output_db_path=output_db_path,
            FAKE_LOGS_PATH=FAKE_LOGS_PATH,
        )
    return inner_fake_build_docker_cmd


class TCGAExomeAlignerTest(FakeS3Mixin, SignpostMixin, PreludeMixin,
                           ZugTestBase):

    def setUp(self):
        super(TCGAExomeAlignerTest, self).setUp()
        self.setup_fake_s3("test")
        self.fake_s3.start()
        self.boto_manager = BotoManager({
            "s3.amazonaws.com": {
                "calling_format": OrdinaryCallingFormat()
            }
        })
        self.fake_s3.stop()
        if sys.platform == "darwin":
            # This is admitedly somewhat aggressive. The reason for
            # this is that when running tests on OSX with boot2docker,
            # mounting volumes from osx -> container (as opposed to
            # boot2docker VM -> container) only works for directories
            # in /Users, so we make a tempdir in the user's homedir
            dir = os.path.expanduser("~/tmp")
            if not os.path.exists(dir):
                raise RuntimeError("Running on OSX without scratch directory {}. "
                                   "Please create this directory so we can create temporary files in it. "
                                   "The reason we need a temporary directory in your home "
                                   "directory is because boot2docker mounting only"
                                   "works for dirctories under /Users".format(dir))
        else:
            dir = None
        os.environ["ALIGNMENT_WORKDIR"] = tempfile.mkdtemp(dir=dir)
        # this is the id for ubuntu:14.04
        os.environ["DOCKER_IMAGE_ID"] = "6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6"
        os.environ["UPLOAD_S3_HOST"] = "s3.amazonaws.com"
        os.environ["BAM_S3_BUCKET"] = "tcga_exome_alignments"
        os.environ["LOGS_S3_BUCKET"] = "tcga_exome_alignment_logs"
        # env vars below here are not actually used
        os.environ["CLEV_ACCESS_KEY"] = "fake"
        os.environ["CEPH_ACCESS_KEY"] = "fake"
        os.environ["CLEV_SECRET_KEY"] = "fake"
        os.environ["CEPH_SECRET_KEY"] = "fake"
        os.environ["SIGNPOST_URL"] = "http://fake-signpost"
        self.fake_s3.start()
        self.boto_manager["s3.amazonaws.com"].create_bucket("tcga_exome_alignments")
        self.boto_manager["s3.amazonaws.com"].create_bucket("tcga_exome_alignment_logs")
        self.fake_s3.stop()

    def get_aligner(self, **kwargs):
        kwargs.update(dict(
            graph=self.graph,
            signpost=self.signpost_client,
            s3=self.boto_manager,
            consul_prefix=self.random_string()+"tcga_exome_align"
        ))
        return TCGAExomeAligner(**kwargs)


    def create_file(self, name, content, aliquot=None):
        bam_doc = self.signpost_client.create()
        assert name.endswith(".bam")
        bam_file = File(
            node_id=bam_doc.did,
            acl=["phs000178"],
            file_name=name,
            md5sum=md5sum_with_size(content)[0],
            file_size=len(content),
            state="live",
            state_comment=None,
            submitter_id=None
        )
        bai_content = "fake_bam_index"
        bai_doc = self.signpost_client.create()
        bai_file = File(
            node_id=bai_doc.did,
            file_name=name+".bai",
            md5sum=md5sum_with_size(bai_content)[0],
            file_size=len(bai_content),
            state="live",
            state_comment=None,
            submitter_id=None
        )
        bam_file.system_annotations = {
            "source": "tcga_cghub",
            "cghub_last_modified": 12345567,
            "cghub_upload_date": 12345567,
            "cghub_legacy_sample_id": aliquot
        }
        with self.graph.session_scope():
            strat = self.graph.nodes(ExperimentalStrategy)\
                              .props(name="WXS").one()
            format = self.graph.nodes(DataFormat)\
                               .props(name="BAM").one()
            platform = self.graph.nodes(Platform)\
                                 .props(name="Illumina GA").one()
            center = self.graph.nodes(Center)\
                               .props(short_name="BI").first()
            bam_file.experimental_strategies = [strat]
            bam_file.platforms = [platform]
            bam_file.data_formats = [format]
            bam_file.related_files = [bai_file]
            bam_file.centers = [center]
        # have to put it in s3
        self.fake_s3.start()
        bucket = self.boto_manager["s3.amazonaws.com"].get_bucket("test")
        key = bucket.new_key(name)
        key.set_contents_from_string(content)
        key = bucket.new_key(name+".bai")
        key.set_contents_from_string(bai_content)
        self.fake_s3.stop()
        # and then the url in signpost
        bam_doc.urls = ["s3://s3.amazonaws.com/test/{}".format(name)]
        bam_doc.patch()
        bai_doc.urls = ["s3://s3.amazonaws.com/test/{}".format(name+".bai")]
        bai_doc.patch()
        return bam_file

    def monkey_patches(self, output_dir="md"):
        aligner_patches = patch.multiple(
            "zug.harmonize.tcga_exome_aligner.TCGAExomeAligner",
            download_inputs=self.with_fake_s3(TCGAExomeAligner.download_inputs),
            upload_file=self.with_fake_s3(TCGAExomeAligner.upload_file),
            build_docker_cmd=fake_build_docker_cmd(output_dir=output_dir),
            get_openstack_uuid=lambda self: None,
        )
        if not os.environ.get("TRAVIS"):
            return aligner_patches
        else:
            # In travis we can't use real docker because the kernel is
            # too old, so we use the FakeDocker client instead, which
            # just execs things on the host
            docker_patch = patch(
                "zug.harmonize.abstract_harmonizer.docker.Client",
                new=FakeDockerClient
            )
            return nested(aligner_patches, docker_patch)

    def test_basic_align(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches():
            aligner = self.get_aligner()
            aligner.go()
        # query for new node and verify pull down from s3
        with self.graph.session_scope():
            new_bam = self.graph.nodes(File)\
                                .filter(File.file_name.astext.endswith(".bam"))\
                                .sysan(source="tcga_exome_alignment")\
                                .one()
            new_bai = self.graph.nodes(File)\
                                .filter(File.file_name.astext.endswith(".bam.bai"))\
                                .sysan(source="tcga_exome_alignment")\
                                .one()
            self.assertEqual(len(new_bam.source_files), 1)
            self.assertEqual(new_bam.related_files, [new_bai])
            new_bam_doc = self.signpost_client.get(new_bam.node_id)
            self.assertEqual(
                new_bam_doc.urls,
                ['s3://s3.amazonaws.com/'
                 'tcga_exome_alignments/{}/test1_gdc_realn.bam'
                 .format(new_bam.node_id)]
            )
            self.assertEqual(new_bam.data_formats[0].name, "BAM")
            self.assertEqual(new_bam.platforms[0].name, "Illumina GA")
            self.assertEqual(new_bam.experimental_strategies[0].name, "WXS")
            edge = self.graph.edges(FileDataFromFile).dst(new_bam.node_id).one()
            self.assertEqual(
                edge.sysan["alignment_docker_image_id"],
                "6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6",
            )
            self.assertEqual(edge.sysan["alignment_reference_name"], "GRCh38.d1.vd1.fa")
            self.assertEqual(edge.sysan["alignment_hostname"], socket.gethostname())
            # shouldn't have failure indicators
            self.assertIsNone(new_bam.source_files[0].sysan.get("alignment_seen_docker_error"))
            self.assertIsNone(new_bam.source_files[0].sysan.get("alignment_last_docker_error_image"))
            self.fake_s3.start()
            bam_key = self.boto_manager.get_url(new_bam_doc.urls[0])
            self.assertEqual(bam_key.get_contents_as_string(), "fake_output_bam")
            # assert that we remove the scratch dir
            self.assertFalse(os.path.isdir(aligner.host_abspath(aligner.config["scratch_dir"])))
            # logs are uploaded compressed
            logs_url = "s3://s3.amazonaws.com/tcga_exome_alignment_logs/aln_{}.log.gz".format(
                file.node_id
            )
            logs_key = self.boto_manager.get_url(logs_url)
            temp_file = StringIO()
            logs_key.get_file(temp_file)
            temp_file.seek(0)
            self.assertEqual(gzip.GzipFile(fileobj=temp_file, mode="rb").read(), FAKE_LOGS_CONTENT)
            self.fake_s3.stop()

    def test_raises_if_no_work(self):
        """It there are no bam files without derived_files, test that we raise
        NoMoreWorkException

        """
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
            second_file = self.get_fuzzed_node(File, state="live")
            file.derived_files = [second_file]
        with self.monkey_patches(), self.assertRaises(NoMoreWorkException):
            aligner = self.get_aligner()
            aligner.go()

    def test_raises_if_consul_key_is_locked(self):
        """It there are no bam files without derived_files, test that we raise
        NoMoreWorkException

        """
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches():
            aligner = self.get_aligner()
            with aligner.consul.consul_session_scope():
                with self.graph.session_scope():
                    aligner.try_lock(file.node_id)
                    # second one should fail because the first locked the only
                    # file to align
                    with self.assertRaises(NoMoreWorkException):
                        aligner.go()

    def test_stop_on_docker_error(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(), self.assertRaises(DockerFailedException):
            aligner = self.get_aligner()
            aligner.config["stop_on_docker_error"] = True
            aligner.docker._fail = True
            aligner.go()
        self.assertTrue(os.path.isdir(aligner.host_abspath(aligner.config["scratch_dir"])))

    @patch('datadog.statsd')
    def test_docker_failure_cleanup(self, mock_statsd):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        aligner = self.get_aligner()
        aligner.inputs = {'bam': file}
        aligner.lock_id = file.node_id
        aligner.docker_image = {"Id": "fake_image_id"}
        aligner.docker_failure_cleanup("fake error logs")
        mock_statsd.event.assert_called_once()

    def test_error_report_on_docker_error(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(), self.assertRaises(RuntimeError):
            aligner = self.get_aligner()
            aligner.docker_failure_cleanup = Mock(name='docker_failure_cleanup')
            aligner.docker._fail = True
            aligner.go()
        aligner.docker_failure_cleanup.assert_called_once()

    def test_marks_file_on_fixmate_failure(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(), self.assertRaises(RuntimeError):
            aligner = self.get_aligner()
            aligner.docker._fail = True
            aligner.docker._logs = ["foo", "bar", "FixMateInformation"]
            aligner.go()
        with self.graph.session_scope():
            file = self.graph.nodes(File).ids(file.node_id).one()
            self.assertTrue(file.sysan["alignment_fixmate_failure"])

    def test_marks_file_on_markdups_failure(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(), self.assertRaises(RuntimeError):
            aligner = self.get_aligner()
            aligner.docker._fail = True
            aligner.docker._logs = ["foo", "bar", "MarkDuplicatesWithMateCigar"]
            aligner.go()
        with self.graph.session_scope():
            file = self.graph.nodes(File).ids(file.node_id).one()
            self.assertTrue(file.sysan["alignment_markdups_failure"])

    def test_marks_file_on_general_failure(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        fake_error_logs = ["fake", "error", "logs"] * 500
        with self.monkey_patches(), self.assertRaises(RuntimeError):
            aligner = self.get_aligner()
            aligner.docker._fail = True
            aligner.docker._logs = fake_error_logs
            aligner.go()
        with self.graph.session_scope():
            file = self.graph.nodes(File).ids(file.node_id).one()
            self.assertTrue(file.sysan["alignment_seen_docker_error"])
            self.assertEqual(
                file.sysan["alignment_last_docker_error_image"],
                '6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6',
            )
            self.assertEqual(
                file.sysan["alignment_last_docker_error_logs"],
                "".join(fake_error_logs)[-1000:]
            )

    def test_doesnt_align_data_problem_files(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
            file.sysan["alignment_data_problem"] = True
        with self.monkey_patches(), self.assertRaises(NoMoreWorkException):
            aligner = self.get_aligner()
            aligner.go()

    def test_size_limit(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches():
            aligner = self.get_aligner()
            with aligner.consul.consul_session_scope():
                aligner.config["size_limit"] = 2
                with self.assertRaises(NoMoreWorkException):
                    with self.graph.session_scope():
                        aligner.find_inputs()
            aligner = self.get_aligner()
            aligner.config["size_limit"] = 50
            with aligner.consul.consul_session_scope():
                with self.graph.session_scope():
                    lock_id, input = aligner.find_inputs()
            assert "bam" in input
            assert "bai" in input

    def test_doesnt_choose_older_files(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
            derived_file = self.get_fuzzed_node(File, state="live")
            file.derived_files = [derived_file]
            older_file = self.create_file("test_older.bam", "more_fake_test_content",
                                          aliquot="foo")
            older_file.sysan["cghub_upload_date"] = 100
        with self.monkey_patches():
            aligner = self.get_aligner()
            with aligner.consul.consul_session_scope():
                with self.assertRaises(NoMoreWorkException):
                    with self.graph.session_scope():
                        aligner.find_inputs()

    def test_chooses_unerrored_files_over_errored(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
            errored_file = self.create_file(
                "test2.bam",
                "more_fake_test_content",
                aliquot="bar"
            )
            errored_file.sysan["alignment_last_docker_error_image"] = "fake_image_id"
            errored_file.sysan["alignment_last_docker_error_logs"] = "fake error logs"
            errored_file.sysan["alignment_seen_docker_error"] = True
        with self.monkey_patches():
            aligner = self.get_aligner()
            with aligner.consul.consul_session_scope():
                with self.graph.session_scope():
                    lock_id, inputs = aligner.find_inputs()
                    self.assertEqual(lock_id, file.node_id)

    def test_chooses_errored_file_if_no_unerrored(self):
        with self.graph.session_scope():
            file1 = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
            file1.sysan["alignment_last_docker_error_image"] = "fake_image_id"
            file1.sysan["alignment_last_docker_error_logs"] = "some fake logs"
            file1.sysan["alignment_seen_docker_error"] = True
        with self.monkey_patches():
            aligner = self.get_aligner()
            with aligner.consul.consul_session_scope():
                with self.graph.session_scope():
                    lock_id, inputs = aligner.find_inputs()
                    self.assertEqual(lock_id, file1.node_id)

    def test_force_input_id(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches():
            aligner = self.get_aligner(force_input_id=file.node_id)
            with self.graph.session_scope(), aligner.consul.consul_session_scope():
                lock_id, inputs = aligner.find_inputs()
                self.assertEqual(inputs["bam"].node_id, file.node_id)

    def test_works_if_output_in_other_directory(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(output_dir="fixmate"):
            aligner = self.get_aligner()
            aligner.go()
        # query for new node and verify pull down from s3
        with self.graph.session_scope():
            new_bam = self.graph.nodes(File)\
                                .filter(File.file_name.astext.endswith(".bam"))\
                                .sysan(source="tcga_exome_alignment")\
                                .one()
            new_bai = self.graph.nodes(File)\
                                .filter(File.file_name.astext.endswith(".bam.bai"))\
                                .sysan(source="tcga_exome_alignment")\
                                .one()
            self.assertEqual(len(new_bam.source_files), 1)
            self.assertEqual(new_bam.related_files, [new_bai])
            new_bam_doc = self.signpost_client.get(new_bam.node_id)
            self.assertEqual(
                new_bam_doc.urls,
                ['s3://s3.amazonaws.com/'
                 'tcga_exome_alignments/{}/test1_gdc_realn.bam'
                 .format(new_bam.node_id)]
            )
            self.assertEqual(new_bam.data_formats[0].name, "BAM")
            self.assertEqual(new_bam.platforms[0].name, "Illumina GA")
            self.assertEqual(new_bam.experimental_strategies[0].name, "WXS")
            edge = self.graph.edges(FileDataFromFile).dst(new_bam.node_id).one()
            self.assertEqual(
                edge.sysan["alignment_docker_image_id"],
                "6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6",
            )
            self.assertEqual(edge.sysan["alignment_reference_name"], "GRCh38.d1.vd1.fa")
            self.assertEqual(edge.sysan["alignment_hostname"], socket.gethostname())
            self.assertEqual(edge.sysan["alignment_last_step"], "fixmate")
            self.fake_s3.start()
            bam_key = self.boto_manager.get_url(new_bam_doc.urls[0])
            self.assertEqual(bam_key.get_contents_as_string(), "fake_output_bam")
            # assert that we remove the scratch dir
            self.assertFalse(os.path.isdir(aligner.host_abspath(aligner.config["scratch_dir"])))
            self.fake_s3.stop()

    def test_tracks_last_step_on_error(self):
        with self.graph.session_scope():
            file = self.create_file("test1.bam", "fake_test_content",
                                    aliquot="foo")
        with self.monkey_patches(), self.assertRaises(RuntimeError):
            aligner = self.get_aligner()
            aligner.docker._fail = True
            aligner.go()
        with self.graph.session_scope():
            file = self.graph.nodes(File).ids(file.node_id).one()
            self.assertEqual(
                file.sysan["alignment_last_docker_error_step"],
                "picard CollectWgsMetrics"
            )
            self.assertEqual(
                file.sysan["alignment_last_docker_error_filename"],
                "/alignment/scratchGRGO1I/realn/bwa_aln_pe/sorted/default.bam"
            )

    def test_will_select_realignment_files(self):
        '''
        Test that files flagged for realignment will be selected.
        '''
        # Create a source and derived bam file that needs realignment.
        with self.graph.session_scope():
            f = self.create_file('some.bam', 'some_content', aliquot='foo')
            o = self.create_file('other.bam', 'other_content', aliquot='foo')

            # Override source sysan to indicate alignment.
            o.sysan['source'] = 'tcga_exome_alignment'

            # Associated the two using an older pipeline.
            FileDataFromFile(
                src=f,
                dst=o,
                system_annotations={
                    'alignment_docker_image_tag': 'pipeline:gdc0.000',
                },
            )

            # Label the source file such that it needs to be realigned.
            f.sysan['qc_failed'] = True

        # Run the mocked aligner to check that it realigns the file.
        with self.monkey_patches():
            aligner = self.get_aligner()
            aligner.go()

        # Verify the presence of a second, realigned derived file.
        with self.graph.session_scope():
            f_realigned = self.graph.nodes(File).ids(f.node_id).one()
            assert(f_realigned.derived_files)
            assert(o in f_realigned.derived_files)
            assert(len(f_realigned.derived_files) == 2)

    def test_old_qc_passed_files_not_realigned(self):
        '''
        Test that old pipeline files that passed qc will not be selected.
        '''
        # Create a source and derived bam file that needs realignment.
        with self.graph.session_scope():
            f = self.create_file('some.bam', 'some_content', aliquot='foo')
            o = self.create_file('other.bam', 'other_content', aliquot='foo')

            # Override source sysan to indicate alignment.
            o.sysan['source'] = 'tcga_exome_alignment'

            # Associated the two using an older pipeline.
            FileDataFromFile(
                src=f,
                dst=o,
                system_annotations={
                    'alignment_docker_image_tag': 'pipeline:gdc0.000',
                },
            )

            # Label the source file such that it does not need to be realigned.
            f.sysan['qc_failed'] = False

        # Run the mocked aligner to check that it realigns the file.
        with self.monkey_patches(), self.assertRaises(NoMoreWorkException):
            aligner = self.get_aligner()
            aligner.go()

        # Verify that no additional derived files were created.
        with self.graph.session_scope():
            f_realigned = self.graph.nodes(File).ids(f.node_id).one()
            assert(f_realigned.derived_files)
            assert(o in f_realigned.derived_files)
            assert(len(f_realigned.derived_files) == 1)

    def test_realignment_happens_only_once(self):
        '''
        Test that realignment won't happen multiple times.
        '''
        # Create a source and derived bam file that needs realignment.
        with self.graph.session_scope():
            f = self.create_file('some.bam', 'some_content', aliquot='foo')
            o = self.create_file('other.bam', 'other_content', aliquot='foo')

            # Override source sysan to indicate alignment.
            o.sysan['source'] = 'tcga_exome_alignment'

            # Associated the two using an older pipeline.
            FileDataFromFile(
                src=f,
                dst=o,
                system_annotations={
                    'alignment_docker_image_tag': 'pipeline:gdc0.000',
                },
            )

            # Label the source file such that it needs to be realigned.
            f.sysan['qc_failed'] = True

        # Run the mocked aligner twice to check that it realigns the file once.
        with self.monkey_patches():
            aligner = self.get_aligner()
            aligner.go()

        with self.monkey_patches(), self.assertRaises(NoMoreWorkException):
            aligner = self.get_aligner()
            aligner.go()

        # Verify the presence of a second, realigned derived file.
        with self.graph.session_scope():
            f_realigned = self.graph.nodes(File).ids(f.node_id).one()
            assert(f_realigned.derived_files)
            assert(o in f_realigned.derived_files)
            assert(len(f_realigned.derived_files) == 2)
