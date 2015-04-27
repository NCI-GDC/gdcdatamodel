#!/usr/bin/env python

from argparse import ArgumentParser
from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer


def main():
    parser = ArgumentParser()
    parser.add_argument("--archive-id", type=str, help="force a specific archvie id")
    args = parser.parse_args()
    syncer = TCGAMAGETABSyncer(archive_id=args.archive_id)
    syncer.sync()

if __name__ == "__main__":
    main()
