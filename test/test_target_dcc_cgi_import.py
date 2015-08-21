import os
import random
from base import ZugTestBase, SignpostMixin, PreludeMixin
from mock import patch
from httmock import urlmatch, all_requests, HTTMock
#from signpostclient import SignpostClient
from zug.datamodel.target.dcc_cgi.tarstream import TarStream, Stream
from zug.datamodel.target.dcc_cgi.s3_wrapper import S3_Wrapper
from zug.datamodel.target.dcc_cgi.target_dcc_cgi_sync import TargetDCCCGIDownloader
from gdcdatamodel import models as mod
from cdisutils.log import get_logger

class TARGETDCCCGIImportTest(SignpostMixin, PreludeMixin, ZugTestBase):
    def setUp(self):
        super(TARGETDCCCGIImportTest, self).setUp()
        os.environ["PG_HOST"] = "localhost"
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASS"] = "test"
        os.environ["PG_NAME"] = "automated_test"
        os.environ["TARGET_PROTECTED_BUCKET"] = "test_tcga_dcc_protected"
        os.environ["TARGET_PUBLIC_BUCKET"] = "test_tcga_dcc_public"
        os.environ["DCC_USER"] = ""
        os.environ["DCC_PASS"] = ""
        os.environ["SIGNPOST_URL"] = "http://localhost"
        os.environ["S3_HOST"] = "ceph.service.consul"
        self.log = get_logger("target_dcc_cgi_project_test_" + str(os.getpid()))
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


        tdc_dl = TargetDCCCGIDownloader(self.signpost_client)

        @all_requests
        def target_mock(url, request):
            self.log.info("Getting: %s" % self.mock_data[url.path])
            content = mock_files[self.mock_data[url.path]]
            return {"content": content, "status_code": 200}

        url_list = []
        with HTTMock(target_mock):
            directory_list = tdc_dl.get_directory_list(self.test_url)
            for entry in directory_list:
                tdc_dl.process_tree(entry['url'], url_list)
                for entry in url_list:
                    expected_links[entry] = True
        
        for key, value in expected_links.iteritems():
            self.assertTrue(value)

    def test_parse_bad_data(self):
        bad_mock_file_data = []

        # load mock data
        for entry in self.mock_bad_data:
            with open(self.FIXTURES_DIR + "/" + entry, "r") as data_file:
                bad_mock_file_data.append(data_file.read())

        tdc_dl = TargetDCCCGIDownloader(self.signpost_client)

        @all_requests
        def target_mock_bad(url, request):
            content = bad_mock_file_data[0]
            return {"content": content, "status_code": 200}

        url_list = []
        with HTTMock(target_mock_bad):
            directory_list = tdc_dl.get_directory_list(self.test_url)
            for entry in directory_list:
                tdc_dl.process_tree(entry['url'], url_list)
        
        self.assertEquals(len(url_list), 0)
        self.assertEquals(len(directory_list), 0)

    def test_check_target_site_down(self):
        tdc_dl = TargetDCCCGIDownloader(self.signpost_client)
        @all_requests
        def target_mock_fail(url, request):
            content = "<HTML>You have no valid data!</HTML>"
            return {"content": content, "status_code": 404}

        url_list = []
        with HTTMock(target_mock_fail):
            self.assertRaises(RuntimeError, tdc_dl.get_directory_list, self.test_url)

    def test_check_target_site_error(self):
        tdc_dl = TargetDCCCGIDownloader(self.signpost_client)
        @all_requests
        def target_mock_error(url, request):
            content = "<HTML>This is an error message</HTML>"
            return {"content": content, "status_code": 500}

        url_list = []
        with HTTMock(target_mock_error):
            self.assertRaises(RuntimeError, tdc_dl.get_directory_list, self.test_url)


    # TEST: create all nodes and edges associated with an archive
    def test_create_nodes_and_edges(self):
        self.assertTrue(True)
        #signpost = SignpostClient(self.signpost_url)
        tdc_cl = TargetDCCCGIDownloader(self.signpost_client)
        tdc_cl.connect_to_psqlgraph()
        tarball_name = "test_tarball.tar.gz"
        tarball_s3_key_name = "test_target_dcc_cgi/" + tarball_name
        tarball_size = 8888
        tarball_md5_sum = "888f213ba97123da0213709aca183888"
        aliquot_submitter_ids = ["TARGET-50-CAAAAM-01A-01R"]
        tag = "OptionAnalysisPipeline2"
        project = "WT"
        experimental_strategy = "WGS"
        platform = "Complete Genomics"
        data_subtype = "CGI Archive"
        node_data = {}
        node_data['participant_barcode'] = "TARGET-50-CAAAAM"
        download_list = []

        dl_entry = {}
        dl_entry['file_name'] = "README.2.1.0.txt"
        dl_entry['url'] = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/%s/%s/EXP/%s" % (
            project, node_data['participant_barcode'], dl_entry['file_name']
        )
        dl_entry['s3_key_name'] = project + "/" + node_data['participant_barcode'] + "/" + dl_entry['file_name']
        dl_entry['md5_sum'] = "9203230090f4a0c3d05aeab528971bae"
        dl_entry['file_size'] = 13930
        download_list.append(dl_entry)

        dl_entry = {}
        dl_entry['file_name'] = "manifest.all.unencrypted"
        dl_entry['url'] = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/%s/%s/EXP/%s" % (
            project, node_data['participant_barcode'], dl_entry['file_name']
        )
        dl_entry['s3_key_name'] = project + "/" + node_data['participant_barcode'] + "/" + dl_entry['file_name']
        dl_entry['md5_sum'] = "9203230090f4a0c3d05aeab528971bae"
        dl_entry['file_size'] = 13930
        download_list.append(dl_entry)
        
        dl_entry = {}
        dl_entry['file_name'] = "manifest.all.unencrypted.sig"
        dl_entry['url'] = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/%s/%s/EXP/%s" % (
            project, node_data['participant_barcode'], dl_entry['file_name']
        )
        dl_entry['s3_key_name'] = project + "/" + node_data['participant_barcode'] + "/" + dl_entry['file_name']
        dl_entry['md5_sum'] = "9203230090f4a0c3d05aeab528971bae"
        dl_entry['file_size'] = 13930
        download_list.append(dl_entry)

        dl_entry = {}
        dl_entry['file_name'] = "manifest.dcc.unencrypted"
        dl_entry['url'] = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/%s/%s/EXP/%s" % (
            project, node_data['participant_barcode'], dl_entry['file_name']
        )
        dl_entry['s3_key_name'] = project + "/" + node_data['participant_barcode'] + "/" + dl_entry['file_name']
        dl_entry['md5_sum'] = "9203230090f4a0c3d05aeab528971bae"
        dl_entry['file_size'] = 13930
        download_list.append(dl_entry)

        # create nodes/edges
        with tdc_cl.psql.session_scope() as session:
            
            # create the file node for the tarball
            tarball_node_id, tarball_file_node = tdc_cl.create_tarball_file_node(
                tarball_name, tarball_md5_sum, tarball_size, 
                tarball_s3_key_name,
                node_data['participant_barcode']
            ) 

            # link the tarball file to the other nodes
            tdc_cl.create_edges(tarball_node_id, tarball_file_node, project, tag)

            # create related files
            for entry in download_list:
                tdc_cl.create_related_file_node(
                    entry, node_data['participant_barcode'],
                    tarball_file_node
            )
        
            # merge in our work
            tdc_cl.psql.current_session().merge(tarball_file_node)
            
            node = tdc_cl.psql.nodes(mod.File).props(file_name=tarball_name).one()

            # check node data
            # check tarball name
            self.assertEqual(node.file_name, tarball_name)

            # check size
            self.assertEqual(node.file_size, tarball_size)

            # check md5 sum
            self.assertEqual(node.md5sum, tarball_md5_sum)

            # check source
            self.assertEqual(node.sysan['source'], "target_dcc_cgi")

            # check edges
            # check that it has all related file
            self.assertEqual(len(node.related_files), 4)

            # check tag
            self.assertEqual(node.tags[0].name, tag)

            # check experimental strategy
            self.assertEqual(node.experimental_strategies[0].name, experimental_strategy)

            # check platform
            self.assertEqual(node.platforms[0].name, platform)

            # check data subtype
            self.assertEqual(node.data_subtypes[0].name, data_subtype)



if __name__ == '__main__':
    cur_test = TARGETDCCCGIImportTest()
    cur_test.test_create_nodes_and_edges()
