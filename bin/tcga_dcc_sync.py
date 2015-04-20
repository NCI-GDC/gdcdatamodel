#!/usr/bin/env python

from argparse import ArgumentParser
from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer


def main():
    parser = ArgumentParser()
    parser.add_argument("--max-memory", type=int, help="maximum size of archive to keep in memory")
    parser.add_argument("--archive-id", type=str, help="force a specific archvie id")
    args = parser.parse_args()
    syncer = TCGADCCArchiveSyncer(
        archive_id=args.archive_id,
        max_memory=args.max_memory,
    )
    syncer.sync()


if __name__ == "__main__":
    main()
