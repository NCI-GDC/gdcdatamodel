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

VALID_PROJECTS_TO_SYNC = [
    "ALL-P1",
    "ALL-P2",
    "AML",
    "AML-IF",
    "CCSK",
    "NBL",
    "OS",
    "RT",
    "WT"
]

def main():
    parser = ArgumentParser()
    parser.add_argument("--local", action="store_true", help="user local object store with tempdir rather than S3")
    parser.add_argument("--num_processes", 
        type=int, 
        help="number of processes to use in process pool", 
        default=environ.get("NUM_PROCESSES", 0)
    )
    parser.add_argument("--pg_host", help="hostname of psqlgraph", default=environ.get("PG_HOST"))
    parser.add_argument("--pg_user", help="psqlgraph username", default=environ.get("PG_USER"))
    parser.add_argument("--pg_pass", help="psqlgraph password", default=environ.get("PG_PASS"))
    parser.add_argument("--pg_name", help="name of psqlgraph db", default=environ.get("PG_NAME"))
    parser.add_argument("--s3_access", help="access key for S3", default=environ.get("S3_ACCESS_KEY"))
    parser.add_argument("--s3_secret", help="secret key for S3", default=environ.get("S3_SECRET_KEY"))
    parser.add_argument("--s3_host", help="hostname for S3", default=environ.get("S3_HOST"))
    parser.add_argument("--signpost_url", help="hostname for signpost_url", default=environ.get("SIGNPOST_URL"))
    parser.add_argument("--dcc_user", help="username for DCC", default=environ.get("DCC_USER"))
    parser.add_argument("--dcc_pass", help="password for DCC", default=environ.get("DCC_PASS"))
    parser.add_argument("--bucket", help="bucket to save files to", default=environ.get("TARGET_PROTECTED_BUCKET"))
    parser.add_argument("--verify_missing", 
        dest="verify_missing",
        help="verify URLs of files before flagging for deletion", 
        action="store_true"
        )

    parser.add_argument("--projects",
        nargs="*", 
        choices=VALID_PROJECTS_TO_SYNC,
        help="project code to sync", 
        default=environ.get("DCC_PROJECT", ["WT"])
    )

    parser.set_defaults(verify_missing=False)
    args = parser.parse_args()
    graph = PsqlGraphDriver(args.pg_host, args.pg_user,
                            args.pg_pass, args.pg_name)
    create_prelude_nodes(graph)

    for project in args.projects:
        syncer = TARGETDCCProjectSyncer(
            project,
            graph_info={
                "host": args.pg_host,
                "user": args.pg_user,
                "pass": args.pg_pass,
                "database": args.pg_name
            },
            storage_info={
                "driver": Local if args.local else S3,
                "access_key": args.s3_access,
                "bucket": args.bucket,
                "kwargs": {
                    "secret": args.s3_secret,
                    "host": args.s3_host,
                    "secure": False
                }
            },
            signpost_url=args.signpost_url,
            dcc_auth=(args.dcc_user, args.dcc_pass),
            pool=Pool(args.num_processes) if args.num_processes else None,
            verify_missing=args.verify_missing
        )
        syncer.sync()
        print "Project complete, %d unclassified files" % syncer.unclassified_files

if __name__ == "__main__":
    main()
