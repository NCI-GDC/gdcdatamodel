#!/usr/bin/env python
from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer
from zug.datamodel.latest_urls import LatestURLParser
from zug.datamodel.prelude import create_prelude_nodes
from argparse import ArgumentParser
from tempfile import mkdtemp
from psqlgraph import PsqlGraphDriver
from signpostclient import SignpostClient

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

Local = get_driver(Provider.LOCAL)
S3 = get_driver(Provider.S3)

from random import shuffle

from functools import partial
from multiprocessing.pool import Pool


def sync_list(args, archives):
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    if args.s3_host:
        storage_client = S3(args.s3_access_key, args.s3_secret_key,
                            host=args.s3_host, secure=False)
    elif args.os_dir:
        storage_client = Local(args.os_dir)
    else:
        storage_client = None
    for archive in archives:
        syncer = TCGADCCArchiveSyncer(
            archive,
            signpost=SignpostClient(args.signpost_url, version="v0"),
            pg_driver=driver,
            dcc_auth=(args.dcc_user, args.dcc_pass),
            scratch_dir=args.scratch_dir,
            storage_client = storage_client,
            meta_only=args.meta_only,
            force=args.force,
            max_memory=args.max_memory,
            no_upload=args.no_upload
        )
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
    parser.add_argument("--signpost-url", type=str, help="signpost url to use")
    parser.add_argument("--dcc-user", type=str, help="username for dcc auth")
    parser.add_argument("--dcc-pass", type=str, help="password for dcc auth")
    parser.add_argument("--meta-only", action="store_true",
                        help="if passed, skip downlading / uploading tarballs")
    parser.add_argument("--force", action="store_true",
                        help="if passed, force sync even if archive appears complete")
    parser.add_argument("--scratch-dir", type=str,
                        help="directory to use as scratch space",
                        default=mkdtemp())
    parser.add_argument("--os-dir", type=str, help="directory to use for mock local object storage",
                        default=mkdtemp())
    parser.add_argument("--archive-name", type=str, help="name of archive to filter to")
    parser.add_argument("--only-unimported", type=str, help="process only archives which have not already been imported")
    parser.add_argument("--s3-host", type=str, help="s3 host to connect to")
    parser.add_argument("--s3-access-key", type=str, help="s3 access key to use")
    parser.add_argument("--s3-secret-key", type=str, help="s3 secret key to use")
    parser.add_argument("--no-upload", action="store_true", help="dont try to upload to object store")
    parser.add_argument("--max-memory", type=int, default=2*10**9,
                        help="maximum size (bytes) of archive to download in memory")
    parser.add_argument("--shuffle", action="store_true",
                        help="shuffle the list of archives before processing")
    parser.add_argument("-p", "--processes", type=int, help="process pool size to use")

    args = parser.parse_args()
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    archives = list(LatestURLParser())
    if args.only_unimported:
        all_archive_nodes = driver.nodes().labels("archive").all()
        imported_names = [a.system_annotations["archive_name"]
                          for a in all_archive_nodes
                          if a.system_annotations.get("archive_name")]
        archives = [a for a in archives
                    if a["archive_name"] not in imported_names]
    if args.archive_name:
        archives = [a for a in archives if a["archive_name"] == args.archive_name]
    if not archives:
        raise RuntimeError("not archive with name {}".format(args.archive_name))
    if args.shuffle:
        shuffle(archives)

    # insert the classification nodes
    create_prelude_nodes(driver)

    if args.processes == 1:
        sync_list(args, archives)
    else:
        pool = Pool(args.processes)
        # this splits the archives list into evenly size chunks
        segments = split(archives, args.processes)
        result = pool.map_async(partial(sync_list, args), segments)
        try:
            result.get(999999)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    main()
