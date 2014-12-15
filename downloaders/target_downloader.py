import logging
import os
import argparse
from downloaders import Downloader
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--download_path', required=True, type=str)
    parser.parse_args()
    args = parser.parse_args()

    downloader = Downloader(
        # gtdownload settings
        cghub_key=os.path.expanduser('~/authorization/jmiller_cghub_key'),
        # filesystem settings
        download_path=args.download_path,
        # Signpost settings
        signpost_host='localhost',
        signpost_port='8080',
        # neo4j settings
        neo4j_host='localhost',
        neo4j_port='7474',
        access_group='phs0004(64|65|66|67|68|69|71)',
        # s3 settings
        s3_auth_path=os.path.expanduser('~/authorization/gdc.yaml'),
        s3_url='192.170.230.172',
        s3_bucket='target_cghub_protected',
    )
    downloader.start()
