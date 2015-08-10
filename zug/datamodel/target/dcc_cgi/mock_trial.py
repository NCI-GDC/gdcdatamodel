#!/usr/bin/env python

import os, sys
from httmock import urlmatch, all_requests, HTTMock
import requests
from target_dcc_cgi_sync import TargetDCCCGIDownloader 

def main():

    test_url = "https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/"
    test_path = "../../../../test/fixtures/target_dcc_cgi/"

    mock_data = {
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

    mock_files = {}

    # load mock data
    for key, value in mock_data.iteritems():
        data = ""
        with open(test_path + value, "r") as data_file:
            mock_files[value] = data_file.read()

    for key in mock_files.keys():
        print key

    tdc_dl = TargetDCCCGIDownloader()

    #auth_data = tdc_dl.get_idpw()
    auth_data = {'id': "", 'pw': ""}

    #@urlmatch(netloc=r'https://target-data.nci.nih.gov/*')
    @all_requests
    def target_mock(url, request):
        print "Getting:", mock_data[url.path]
        content = mock_files[mock_data[url.path]]
        return {"content": content, "status_code": 200}

    url_list = []
    with HTTMock(target_mock):
        directory_list = tdc_dl.get_directory_list(test_url, auth_data)
        for entry in directory_list:
            print entry

    #directory_list2 = tdc_dl.get_directory_list(test_url, auth_data)
    #for entry in directory_list2:
    #    print entry

    #sys.exit()

    with HTTMock(target_mock):
        for entry in directory_list:
            tdc_dl.process_tree(entry['url'], auth_data, url_list)
            for entry in url_list:
                print entry
        #r = requests.get("https://target-data.nci.nih.gov/WT/Discovery/WGS/CGI/OptionAnalysisPipeline2/data1.txt")
        #print r.content


main()
