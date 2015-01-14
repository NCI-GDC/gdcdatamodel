from zug.datamodel.tcga_dcc_sync import TCGADCCArchiveSyncer, insert_classification_nodes
from zug.datamodel.latest_urls import LatestURLParser
from argparse import ArgumentParser
from tempfile import mkdtemp
from psqlgraph import PsqlGraphDriver

from random import shuffle

from functools import partial
from multiprocessing.pool import Pool


def sync_list(args, archives):
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    syncer = TCGADCCArchiveSyncer(args.signpost_url, driver,
                                  (args.dcc_user, args.dcc_pass),
                                  args.scratch_dir)
    try:
        syncer.sync_archives(archives)
    except:
        syncer.log.exception("caught exception while syncing")
        raise


def split(a, n):
    """Split a into n evenly sized chunks"""
    k, m = len(a) / n, len(a) % n
    return [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n)]


def main():
    parser = ArgumentParser()
    parser.add_argument("--pg-host", type=str, help="postgres database host")
    parser.add_argument("--pg-user", type=str, help="postgres database user")
    parser.add_argument("--pg-pass", type=str, help="postgres database password")
    parser.add_argument("--pg-database", type=str, help="the postgre database to connect to")
    parser.add_argument("--signpost-url", type=str, help="signpost url to use")
    parser.add_argument("--dcc-user", type=str, help="username for dcc auth")
    parser.add_argument("--dcc-pass", type=str, help="password for dcc auth")
    parser.add_argument("--scratch-dir", type=str, help="directory to use as scratch space",
                        default=mkdtemp())
    parser.add_argument("-p", "--processes", type=int, help="process pool size to use")

    args = parser.parse_args()
    archives = list(LatestURLParser())
    shuffle(archives)
    pool = Pool(args.processes)

    # insert the classification nodes
    driver = PsqlGraphDriver(args.pg_host, args.pg_user,
                             args.pg_pass, args.pg_database)
    insert_classification_nodes(driver)

    if args.processes == 1:
        sync_list(args, archives)
    else:
        # this splits the archives list into evenly size chunks
        segments = split(archives, args.processes)
        result = pool.map_async(partial(sync_list, args), segments)
        try:
            result.get(999999)
        except KeyboardInterrupt:
            print "killed"
            return


if __name__ == "__main__":
    main()
