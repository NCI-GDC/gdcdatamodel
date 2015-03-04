#!/usr/bin/env python

from os import environ
from zug.datamodel.target.sample_matrices import TARGETSampleMatrixSyncer
from zug.datamodel.prelude import create_prelude_nodes
from psqlgraph import PsqlGraphDriver


PROJECTS_TO_IMPORT = [
    "ALL-P1",
    # still has conflicting information between participant / aliquot in some rows
    # "ALL-P2",
    "AML",
    "AML-IF",
    "CCSK",
    "NBL",
    "OS",
    "RT",
    "WT"
]


def main():
    graph = PsqlGraphDriver(environ["ZUGS_PG_HOST"], environ["ZUGS_PG_USER"],
                            environ["ZUGS_PG_PASS"], environ["ZUGS_PG_NAME"])
    create_prelude_nodes(graph)
    for project in PROJECTS_TO_IMPORT:
        syncer = TARGETSampleMatrixSyncer(project, graph=graph,
                                          dcc_auth=(environ["ZUGS_DCC_USER"], environ["ZUGS_DCC_PASS"]))
        syncer.sync()


if __name__ == "__main__":
    main()
