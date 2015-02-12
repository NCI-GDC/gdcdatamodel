#!/usr/bin/env python

from zug.datamodel.tcga_magetab_sync import TCGAMAGETABSyncer
from zug.datamodel.latest_urls import LatestURLParser
from functools import partial
from argparse import ArgumentParser
from psqlgraph import PsqlGraphDriver
from multiprocessing import Pool


def sync_magetab_list(args, archives):
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    for archive in archives:
        try:
            syncer = TCGAMAGETABSyncer(archive, pg_driver=driver)
        except Exception:
            continue
        try:
            syncer.sync()  # ugh
        except Exception:  # we use Exception so as not to catch KeyboardInterrupt et al.
            syncer.log.exception("caught exception while syncing")


def split(a, n):
    """Split a into n evenly sized chunks"""
    k, m = len(a) / n, len(a) % n
    return [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
            for i in xrange(n)]


def main():
    parser = ArgumentParser()
    parser.add_argument("--pg-host", type=str, help="postgres database host")
    parser.add_argument("--pg-user", type=str, help="postgres database user")
    parser.add_argument("--pg-pass", type=str, help="postgres database password")
    parser.add_argument("--pg-database", type=str,
                        help="the postgres database to connect to")
    parser.add_argument("--archive-name", type=str, help="name of archive to filter to")
    parser.add_argument("--processes", type=int, default=1, help="number of processes to use")
    args = parser.parse_args()
    archives = [a for a in LatestURLParser() if a["data_level"] == "mage-tab"]
    if args.archive_name:
        archives = [a for a in archives if a["archive_name"] == args.archive_name]
    if args.processes == 1:
        sync_magetab_list(args, archives)
    else:
        pool = Pool(args.processes)
        segments = split(archives, args.processes)
        result = pool.map_async(partial(sync_magetab_list, args), segments)
        try:
            result.get(999999)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    main()
