#!/usr/bin/env python
from os import environ
import os
import sys
import tempfile
from argparse import ArgumentParser
import requests
from bs4 import BeautifulSoup
import urllib3
import logging

#from zug.datamodel.prelude import create_prelude_nodes
from cdisutils.log import get_logger
from zug.datamodel.target.clinical import TARGETClinicalSyncer, process_tree, find_clinical, PROJECTS_TO_SYNC
from psqlgraph import PsqlGraphDriver

# this disables the warnings from requests about verify=False
urllib3.disable_warnings()
logging.captureWarnings(True)

def main():
    # parse args, if any
    parser = ArgumentParser()
    parser.add_argument("--pg_host", help="hostname of psqlgraph", default=environ.get("PG_HOST"))
    parser.add_argument("--pg_user", help="psqlgraph username", default=environ.get("PG_USER"))
    parser.add_argument("--pg_pass", help="psqlgraph password", default=environ.get("PG_PASS"))
    parser.add_argument("--pg_name", help="name of psqlgraph db", default=environ.get("PG_NAME"))
    parser.add_argument("--dcc_user", help="username for DCC", default=environ.get("DCC_USER"))
    parser.add_argument("--dcc_pass", help="password for DCC", default=environ.get("DCC_PASS"))
    parser.add_argument("--projects",
        nargs="*",
        choices=PROJECTS_TO_SYNC.keys(),
        help="project code to sync",
        default=environ.get("DCC_PROJECT", PROJECTS_TO_SYNC.keys()[-1])
    )
    args = parser.parse_args()
    
    log = get_logger("target_clinical_sync_bin_{}".format(os.getpid()))

    # connect to psqlgraph
    graph = PsqlGraphDriver(args.pg_host, args.pg_user,
                            args.pg_pass, args.pg_name)

    clinical_data = find_clinical(args)

    log.info("%d clinical data files found" % len(clinical_data))

    for project, url_list in clinical_data.iteritems():
        for url in url_list:
            log.info("Syncing %s", url['url'])
            syncer = TARGETClinicalSyncer(
                project,
                url['url'], 
                graph,
                dcc_auth=(args.dcc_user, args.dcc_pass),
            )
            syncer.sync()

if __name__ == "__main__":
    main()
