import os
import tempfile
from uuid import uuid4

from mock import patch
from base import ZugsTestBase, FakeS3Mixin

from zug.harmonize.tcga_exome_aligner import TCGAExomeAligner
# TODO really need to find a better place for this
from zug.downloaders import md5sum_with_size
from cdisutils.net import BotoManager
from gdcdatamodel.models import File, Aliquot, ExperimentalStrategy

from boto.s3.connection import OrdinaryCallingFormat


class TCGAExomeAlignerTest(ZugsTestBase, FakeS3Mixin):

    def setUp(self):
        super(TCGAExomeAlignerTest, self).setUp()
        # s3
        self.setup_fake_s3("test")
        self.fake_s3.start()
        self.boto_manager = BotoManager({
            "s3.amazonaws.com": {
                "calling_format": OrdinaryCallingFormat()
            }
        })
        self.fake_s3.stop()
        # env vars
        os.environ["ALIGNMENT_WORKDIR"] = tempfile.mkdtemp()
        # this is the id for ubuntu:14.04
        os.environ["DOCKER_IMAGE_ID"] = "6d4946999d4fb403f40e151ecbd13cb866da125431eb1df0cdfd4dc72674e3c6"


    def get_aligner(self):
        return TCGAExomeAligner(
            graph=self.graph,
            signpost=self.signpost_client,
            s3=self.boto_manager
        )

    def create_file(self, name, content):
        doc = self.signpost_client.create()
        file = File(
            node_id=doc.did,
            file_name=name,
            md5sum=md5sum_with_size(content)[0],
            file_size=len(content),
            state="live",
            state_comment=None,
            submitter_id=None
        )
        file.system_annotations = {
            "source": "tcga_cghub",
            "cghub_last_modified": 12345567
        }
        with self.graph.session_scope():
            strat = self.graph.nodes(ExperimentalStrategy)\
                              .props(name="WXS").one()
            file.experimental_strategies = [strat]
        # have to put it in s3
        self.fake_s3.start()
        bucket = self.boto_manager["s3.amazonaws.com"].get_bucket("test")
        key = bucket.new_key(name)
        key.set_contents_from_string(content)
        self.fake_s3.stop()
        # and then the url in signpost
        doc.urls = ["s3://s3.amazonaws.com/test/{}".format(name)]
        doc.patch()
        return file

    def create_aliquot(self):
        aliquot = Aliquot(
            node_id=str(uuid4()),
            submitter_id="fake_barcode",
            source_center="foo",
            amount=3.5,
            concentration=10.0
        )
        return aliquot

    def monkey_patches(self):
        return patch.multiple(
            "zug.harmonize.tcga_exome_aligner.TCGAExomeAligner",
            download_input_bam=self.with_fake_s3(TCGAExomeAligner.download_input_bam))

    def test_basic_align(self):
        with self.graph.session_scope():
            aliquot = self.create_aliquot()
            file = self.create_file("test1", "fake_test_content")
            file.aliquots = [aliquot]
        aligner = self.get_aligner()
        with self.monkey_patches():
            aligner.align()
