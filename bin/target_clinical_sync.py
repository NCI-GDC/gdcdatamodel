#!/usr/bin/env python
from os import environ
import sys
import os
import tempfile
from argparse import ArgumentParser
from multiprocessing import Pool
import requests

import urllib3
import logging
from cdisutils.log import get_logger
from zug.datamodel.target.clinical import find_spreadsheets

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

def main():
    # parse args, if any
    parser = ArgumentParser()
    args = parser.parse_args()
    log = get_logger("target_clinical_sync_" + str(os.getpid()))

    spreadsheet_data = find_spreadsheets(PROJECTS_TO_SYNC, BASE_URL)

    print spreadsheet_data

    # connect to psqlgraph
    graph = PsqlGraphDriver(environ["PG_HOST"], environ["PG_USER"],
                            environ["PG_PASS"], environ["PG_NAME"])

    for project, url_list in spreadsheet_data.iteritems():
        for url in url_list:
            log.info("Syncing %s" % url['url'])
            syncer = TARGETClinicalSyncer(
                project,
                url['url'], 
                graph,
                dcc_auth=(environ["DCC_USER"], environ["DCC_PASS"]),
            )
            syncer.sync()

if __name__ == "__main__":
    main()

