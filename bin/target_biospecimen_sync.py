#!/usr/bin/env python

from os import environ
from zug.datamodel.target.sample_matrices import TARGETSampleMatrixSyncer
from zug.datamodel.prelude import create_prelude_nodes
from psqlgraph import PsqlGraphDriver
from argparse import ArgumentParser

PROJECTS_TO_IMPORT = [
    # we are only doing WT for now
    # "ALL-P1",
    # "ALL-P2",
    "ALL",
    "AML",
    "AML-IF",
    "CCSK",
    "NBL",
    "OS",
    "RT",
    "WT"
]

def parse_cmd_args():
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
        choices=PROJECTS_TO_IMPORT,
        help="project code to sync",
        default=environ.get("DCC_PROJECT", [PROJECTS_TO_IMPORT[-1]])
    )
    args = parser.parse_args()

    return args

def main():
    args = parse_cmd_args()
    graph = PsqlGraphDriver(args.pg_host, args.pg_user,
                            args.pg_pass, args.pg_name)
    create_prelude_nodes(graph)
    for project in args.projects:
        syncer = TARGETSampleMatrixSyncer(project, graph=graph,
                                          dcc_auth=(args.dcc_user, args.dcc_pass))
        syncer.sync()


if __name__ == "__main__":
    main()
