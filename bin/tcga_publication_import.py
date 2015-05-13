#!/usr/bin/env python
from zug.datamodel.tcga_publication_import import TCGAPublicationImporter


def main():
    publication_importer = TCGAPublicationImporter()
    publication_importer.run()


if __name__ == '__main__':
    main()
