#!/usr/bin/env python
from os import environ
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator


def main():
    g = PsqlGraphDriver(environ["ZUGS_PG_HOST"], environ["ZUGS_PG_USER"],
                        environ["ZUGS_PG_PASS"], environ["ZUGS_PG_NAME"],
                        edge_validator=AvroEdgeValidator(edge_avsc_object),
                        node_validator=AvroNodeValidator(node_avsc_object))
    publication_importer = TCGAPublicationImporter(g)
    publication_importer.run()


if __name__ == '__main__':
    main()
