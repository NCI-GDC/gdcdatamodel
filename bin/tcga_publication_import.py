#!/usr/bin/env python
from os import environ
from cdisutils.log import get_logger
from psqlgraph import PsqlGraphDriver
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter
import argparse
from gdcdatamodel import node_avsc_object, edge_avsc_object
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator


def main():
    logger = get_logger("tcga_publication_build")
    g = PsqlGraphDriver(environ["ZUGS_PG_HOST"], environ["ZUGS_PG_USER"],
                        environ["ZUGS_PG_PASS"], environ["ZUGS_PG_NAME"],
                        edge_validator=AvroEdgeValidator(edge_avsc_object),
                        node_validator=AvroNodeValidator(node_avsc_object))
    parser = argparse.ArgumentParser()
    # This is a list of bam files given by Mark Jensen
    parser.add_argument('--bamlist', type=str, help='filename of bamfile list',
                        required=True)
    parse = parser.parse_args()
    bams = []
    with open(parse.bamlist, 'r') as f:
        firstline = True
        for line in f.readlines():
            if firstline:
                firstline = False
            else:
                items = line.split()
                disease = items[0]
                if len(items) == 5:
                    filename = items[-1]
                    analysis_id = items[-2]
                    bams.append({'filename': filename,
                                 'analysis_id': analysis_id,
                                 'disease': disease})
                else:
                    filename = items[-1]
                    bams.append({'filename': filename,
                                 'disease': disease})
    publication_importer = TCGAPublicationImporter(bams,
                                                   g, logger)
    publication_importer.run()


if __name__ == '__main__':
    main()
