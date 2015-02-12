#!/usr/bin/env python

from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer
from zug.datamodel.latest_urls import LatestURLParser
from argparse import ArgumentParser
from psqlgraph import PsqlGraphDriver


def sync_magetab(args, archive):
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    syncer = TCGAMAGETABSyncer(archive, pg_driver=driver)
    try:
        syncer.sync()  # ugh
    except Exception:  # we use Exception so as not to catch KeyboardInterrupt et al.
        syncer.log.exception("caught exception while syncing")


def main():
    parser = ArgumentParser()
    parser.add_argument("--pg-host", type=str, help="postgres database host")
    parser.add_argument("--pg-user", type=str, help="postgres database user")
    parser.add_argument("--pg-pass", type=str, help="postgres database password")
    parser.add_argument("--pg-database", type=str,
                        help="the postgres database to connect to")
    parser.add_argument("--archive-name", type=str, help="name of archive to filter to")
    args = parser.parse_args()
    archives = [a for a in LatestURLParser() if a["data_level"] == "mage-tab"]
    if args.archive_name:
        archives = [a for a in archives if a["archive_name"] == args.archive_name]
    for archive in archives:
        sync_magetab(args, archive)

if __name__ == "__main__":
    main()
