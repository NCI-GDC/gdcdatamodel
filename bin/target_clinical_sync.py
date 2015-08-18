#!/usr/bin/env python
from os import environ
import sys
import tempfile
from argparse import ArgumentParser
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import urllib3
import logging

#from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel.target.clinical import TARGETClinicalSyncer
from psqlgraph import PsqlGraphDriver

# this disables the warnings from requests about verify=False
urllib3.disable_warnings()
logging.captureWarnings(True)


BASE_URL = "https://target-data.nci.nih.gov/"

PROJECTS_TO_SYNC = { 
    # "ALL-P1",
    # "ALL-P2",
    #"ALL/Phase_I" : "/Discovery/clinical/harmonized/",  # temp
    #"ALL/Phase_II" : "/Discovery/clinical/harmonized/", # temp
    "AML" : "/Discovery/clinical/harmonized/",
    #"AML-IF" : "/Discovery/clinical/",                  # temp
    #"CCSK" : "/Discovery/clinical/harmonized/",         # temp
    "NBL" : "/Discovery/clinical/harmonized/",
    #"OS" : "/Discovery/clinical/",                      # temp
    #"RT" : "/Discovery/clinical/harmonized/",           # temp
    "WT" : "/Discovery/clinical/harmonized/"
}

# NB: we're looking for them now, but these are the first ones I was told
# to use, so I'm keeping this as a sanity check
URLS_TO_CHECK = {
    "AML": [ "https://target-data.nci.nih.gov/AML/Discovery/clinical/harmonized/TARGET_AML_ClinicalData_5_8_2015_harmonized.xlsx" ],
    "NBL": [ "https://target-data.nci.nih.gov/NBL/Discovery/clinical/harmonized/TARGET_NBL_discovery_and_validataion_ClinicalData_5_8_2015_harmonized.xlsx" ],
    "WT": [ "https://target-data.nci.nih.gov/WT/Discovery/clinical/harmonized/TARGET_WT_ClinicalData_1_21_2015_public_harmonized.xlsx", 
    "https://target-data.nci.nih.gov/WT/Discovery/clinical/harmonized/TARGET_WT_ClinicalData_1_21_2015_protected_harmonized.xlsx" ]
}
ROW_CLASSES = [ "even", "odd" ]

def process_tree(url):
    """Walk the given url and recursively find all the spreadsheet links."""
    url_list = []
    r = requests.get(url, auth=(environ["QA_DCC_USER"], environ["QA_DCC_PASS"]), verify=False)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "lxml")
        file_table = soup.find('table', attrs={'id':'indexlist'})
        rows = file_table.find_all('tr')
        for row in rows:
            if row['class'][0] in ROW_CLASSES:
                image = row.find('img')
                if image['alt'].find("DIR") == -1:
                    dir_data = {}
                    dir_data['dir_name'] = row.find('td', class_="indexcolname").get_text().strip()
                    link = row.find('a')
                    if (link['href'].find("xlsx") != -1) and (link['href'].find("Clinical") != -1):
                        dir_data['url'] = url + link['href']
                        url_list.append(dir_data)

    return url_list

def find_spreadsheets():
    spreadsheet_urls = {}
    for project, url_loc in PROJECTS_TO_SYNC.iteritems():
        url = "%s%s%s" % (
            BASE_URL,
            project,
            url_loc
        )
        spreadsheets = process_tree(url)
        spreadsheet_urls[project] = spreadsheets

    for key, value in spreadsheet_urls.iteritems():
        for entry in value:
    
    return spreadsheet_urls

def main():
    # parse args, if any
    parser = ArgumentParser()
    args = parser.parse_args()

    # connect to psqlgraph
    graph = PsqlGraphDriver(environ["QA_PG_HOST"], environ["QA_PG_USER"],
                            environ["QA_PG_PASS"], environ["QA_PG_NAME"])

    spreadsheet_data = find_spreadsheets()

    for project, url_list in spreadsheet_data.iteritems():
        for url in url_list:
            print "Syncing", url['url']
            syncer = TARGETClinicalSyncer(
                project,
                url['url'], 
                graph,
                dcc_auth=(environ["QA_DCC_USER"], environ["QA_DCC_PASS"]),
            )
            syncer.sync()

if __name__ == "__main__":
    main()

