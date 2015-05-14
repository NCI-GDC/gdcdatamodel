#!/usr/bin/env python
from os import environ
from cdisutils.log import get_logger
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_dcc_sync import TCGADCCEdgeBuilder

from gdcdatamodel.models import File

"""NOTE (jjp, 03/05/15) in principle this script will never need to
be run again in the future, since the importers now take care of all
of this.
"""


def main():
    logger = get_logger("tcga_edge_build")
    g = PsqlGraphDriver(environ["ZUGS_PG_HOST"], environ["ZUGS_PG_USER"],
                        environ["ZUGS_PG_PASS"], environ["ZUGS_PG_NAME"])
    with g.session_scope():
        without_center_q = g.nodes(File).sysan({"source": "tcga_dcc"})\
                                        .except_(g.nodes(File).sysan({"source": "tcga_dcc"}).path_out("center"))
        without_platform_q = g.nodes(File).sysan({"source": "tcga_dcc"})\
                                          .except_(g.nodes(File).sysan({"source": "tcga_dcc"}).path_out("platform"))
        logger.info("loading edges to process")
        nodes = without_center_q.union(without_platform_q).all()
        logger.info("loaded %s edges to process", len(nodes))
        for node in nodes:
            assert node.system_annotations["source"] == "tcga_dcc"
            builder = TCGADCCEdgeBuilder(node, g, logger)
            logger.info("building edges for %s", node)
            builder.build()

if __name__ == "__main__":
    main()
