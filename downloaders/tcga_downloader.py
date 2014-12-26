#!/usr/bin/python

import os
import argparse
from downloaders import Downloader

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--ddir', required=True, type=str,
                        help='download directory')
    parser.add_argument('-n', '--name', required=True, type=str,
                        help='name of the downloaders')
    parser.add_argument('-g', '--gateway', default='192.170.230.173', type=str,
                        help='s3 gateway host')
    parser.add_argument('-c', '--cypher', default='', type=str,
                        help='extra condition to append to cypher query')
    parser.add_argument('-i', '--id', default='', type=str,
                        help='the id to try and force the downloader to resume with')
    parser.add_argument('--resume', default=False, type=bool,
                        help='try and resume a file')
    args = parser.parse_args()

    downloader = Downloader(
        # General
        name=args.name,

        # gtdownload settings
        cghub_key=os.path.expanduser('~/authorization/jmiller_cghub_key'),

        # filesystem settings
        download_path=args.ddir,

        # Signpost settings
        signpost_host='localhost',
        signpost_port='8080',

        # neo4j settings
        neo4j_host='10.64.0.141',
        neo4j_port='7474',
        access_group='phs000178',
        force_resume_id=args.id,
        extra_cypher=args.cypher,
        resume=args.resume,

        # s3 settings
        s3_auth_path=os.path.expanduser('~/authorization/gdc.yaml'),
        s3_url=args.gateway,
        s3_bucket='tcga_cghub_protected',
    )
    downloader.start()
