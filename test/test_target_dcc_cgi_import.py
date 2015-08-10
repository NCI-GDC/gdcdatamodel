import os
import random
from base import ZugTestBase
from mock import patch
from httmock import urlmatch, all_requests, HTTMock
from signpostclient import SignpostClient
from zug.datamodel.target.dcc_cgi.tarstream import TarStream, Stream
from zug.datamodel.target.dcc_cgi.s3_wrapper import S3_Wrapper
from zug.datamodel.target.dcc_cgi.target_dcc_cgi_sync import TargetDCCCGIDownloader
from gdcdatamodel import models as mod

class TARGETDCCCGIImportTest(ZugTestBase):
    def setUp(self):
        super(TARGETDCCCGIImportTest, self).setUp()
        #self.storage_client.create_containter("test_target_dcc_cgi_protected")
        os.environ["PG_HOST"] = "localhost"
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASS"] = "test"
        os.environ["PG_NAME"] = "automated_test"
        #os.environ["SIGNPOST_URL"] = self.signpost_url
        #os.environ["SCRATCH_DIR"] = self.scratch_dir
        os.environ["TARGET_PROTECTED_BUCKET"] = "test_tcga_dcc_protected"
        os.environ["TARGET_PUBLIC_BUCKET"] = "test_tcga_dcc_public"
        os.environ["DCC_USER"] = ""
        os.environ["DCC_PASS"] = ""
        self.TEST_DIR = os.path.dirname(os.path.realpath(__file__))
        self.FIXTURES_DIR = os.path.join(self.TEST_DIR, "fixtures", "target_dcc_cgi")
        self.test_url = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/"
        self.mock_data = {
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/': "page1.html",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/': "page2.html",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/': "page3.html",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/README.2.1.0.txt': "readme.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted': "manifest1.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted.sig': "manifest2.sig",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.dcc.unencrypted': "manifest3.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/': "page4.html",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/externalSampleId-GS008280DNA_A01': "externalSample1.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/idMap-GS000010157-ASM-T1.tsv': "idMap1.tsv",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/version': "version1.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/': "page5.html",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/externalSampleId-GS008280DNA_A01': "externalSample2.txt",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/idMap-GS000010157-ASM-T1.tsv': "idMap2.tsv",
            '/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/version': "version2.txt",
        }
        self.mock_bad_data = [
            "page1_bad1.html",
            "page1_bad2.html",
            "page1_bad3.html"
        ] 

    def test_create_download_list(self):

        mock_files = {}
        expected_links = {
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/externalSampleId-GS00828-DNA_A01": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/idMap-GS000010157-ASM-T1.tsv": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/version": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/externalSampleId-GS00828-DNA_E05": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/idMap-GS000010157-ASM-N1.tsv": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-10A-01D/version": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/README.2.1.0.txt": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted.sig": False,
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.dcc.unencrypted": False,
        }

        # load mock data
        for key, value in self.mock_data.iteritems():
            data = ""
            with open(self.FIXTURES_DIR + "/" + value, "r") as data_file:
                mock_files[value] = data_file.read()

        #for key in mock_files.keys():
        #    print key

        tdc_dl = TargetDCCCGIDownloader()

        #auth_data = tdc_dl.get_idpw()
        auth_data = {'id': "", 'pw': ""}

        #@urlmatch(netloc=r'https://target-data.nci.nih.gov/*')
        @all_requests
        def target_mock(url, request):
            print "Getting:", self.mock_data[url.path]
            content = mock_files[self.mock_data[url.path]]
            return {"content": content, "status_code": 200}

        url_list = []
        with HTTMock(target_mock):
            directory_list = tdc_dl.get_directory_list(self.test_url, auth_data)
            for entry in directory_list:
                tdc_dl.process_tree(entry['url'], auth_data, url_list)
                for entry in url_list:
                    expected_links[entry] = True
        
        for key, value in expected_links.iteritems():
            print key, value
            self.assertTrue(value)

    def test_parse_bad_data(self):

        bad_mock_file_data = []

        # load mock data
        for entry in self.mock_bad_data:
            with open(self.FIXTURES_DIR + "/" + entry, "r") as data_file:
                bad_mock_file_data.append(data_file.read())

        tdc_dl = TargetDCCCGIDownloader()

        auth_data = {'id': "", 'pw': ""}

        @all_requests
        def target_mock_bad(url, request):
            content = bad_mock_file_data[0]
            return {"content": content, "status_code": 200}

        url_list = []
        with HTTMock(target_mock_bad):
            directory_list = tdc_dl.get_directory_list(self.test_url, auth_data)
            for entry in directory_list:
                tdc_dl.process_tree(entry['url'], auth_data, url_list)
        
        self.assertEquals(len(url_list), 0)
        self.assertEquals(len(directory_list), 0)

    def test_check_target_site_down(self):
        tdc_dl = TargetDCCCGIDownloader()
        auth_data = {'id': "", 'pw': ""}
        @all_requests
        def target_mock_fail(url, request):
            content = "<HTML>You have no valid data!</HTML>"
            return {"content": content, "status_code": 404}

        url_list = []
        with HTTMock(target_mock_fail):
            directory_list = tdc_dl.get_directory_list(self.test_url, auth_data)
            self.assertEqual(len(directory_list), 0)
        self.assertTrue(True)

    def test_check_target_site_error(self):
        tdc_dl = TargetDCCCGIDownloader()
        auth_data = {'id': "", 'pw': ""}
        @all_requests
        def target_mock_error(url, request):
            content = "<HTML>This is an error message</HTML>"
            return {"content": content, "status_code": 500}

        url_list = []
        with HTTMock(target_mock_error):
            directory_list = tdc_dl.get_directory_list(self.test_url, auth_data)
            self.assertEqual(len(directory_list), 0)
        self.assertTrue(True)



# TEST: stream files into an archive and put it on the object store
    def test_stream_create_archive_on_os(self):
        files = [
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted",
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/README.2.1.0.txt",
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.all.unencrypted.sig",
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/manifest.dcc.unencrypted", 
            "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/TARGET-50-CAAAAH/EXP/TARGET-50-CAAAAH-01A-01D/ASM/EVIDENCE-GS000010157-ASM-N1/evidenceDnbs-chr1-GS000010157-ASM-T1.tsv.bz2"]

        self.assertEqual(True, True)


# TEST: create all nodes and edges associated with an archive
    def test_create_nodes_and_edges(self):
        self.assertTrue(True)
        #signpost = SignpostClient()
        #tdc_cl = TargetDCCCGIDownloader()
        #pq = tdc_cl.connect_to_psqlgraph()
        #tarball_name = "test_tarball.tar.gz"
        #tarball_s3_key_name = "test_target_dcc_cgi/" + tarball_name
        #with pq.session_scope() as session:
        #    tarball_node_id = self.create_tarball_file_node(signpost, tarball_name, tarball_s3_key_name) 

            # check for existence of nodes
        #    nodes = pq.nodes(mod.File).props(name=tarball_name).all()
        #    self.assertEqual(len(nodes), 1)


if __name__ == '__main__':
    cur_test = TARGETDCCCGIImportTest()
    cur_test.test_create_nodes_and_edges()
