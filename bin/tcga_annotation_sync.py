#!/usr/bin/env python

from zug.datamodel.tcga_annotations import TCGAAnnotationSyncer


def main():
    syncer = TCGAAnnotationSyncer()
    syncer.go()

if __name__ == "__main__":
    main()
