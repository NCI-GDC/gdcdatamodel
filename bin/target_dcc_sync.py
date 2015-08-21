#!/usr/bin/env python
from os import environ
import tempfile
from argparse import ArgumentParser
from multiprocessing import Pool

from zug.datamodel.prelude import create_prelude_nodes
from zug.datamodel.target.dcc_sync import TARGETDCCProjectSyncer
from psqlgraph import PsqlGraphDriver

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

Local = get_driver(Provider.LOCAL)
S3 = get_driver(Provider.S3)


PROJECTS_TO_SYNC = [
    # we are only doing WT for now
    # "ALL-P1",
    # "ALL-P2",
    #"AML",
    # "AML-IF",
    # "CCSK",
    #"NBL",
    # "OS",
    # "RT",
    "WT"
]


def main():
    parser = ArgumentParser()
    parser.add_argument("--local", action="store_true", help="user local object store with tempdir rather than S3")
    parser.add_argument("--pool", type=int, help="number of processes to use in process pool", default=0)
    args = parser.parse_args()

    graph = PsqlGraphDriver(environ["PG_HOST"], environ["PG_USER"],
                            environ["PG_PASS"], environ["PG_NAME"])
    create_prelude_nodes(graph)

    for project in PROJECTS_TO_SYNC:
        syncer = TARGETDCCProjectSyncer(
            project,
            graph_info={
                "host": environ["PG_HOST"],
                "user": environ["PG_USER"],
                "pass": environ["PG_PASS"],
                "database": environ["PG_NAME"]
            },
            storage_info={
                "driver": Local if args.local else S3,
                "access_key": environ["S3_ACCESS_KEY"],
                "kwargs": {
                    "secret": environ.get("S3_SECRET_KEY"),
                    "host": environ.get("S3_HOST"),
                    "secure": False
                }
            },
            signpost_url=environ["SIGNPOST_URL"],
            dcc_auth=(environ["DCC_USER"], environ["DCC_PASS"]),
            pool=Pool(args.pool) if args.pool else None
        )
        syncer.sync()

if __name__ == "__main__":
    main()
