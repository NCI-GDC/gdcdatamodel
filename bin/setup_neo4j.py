#!/user/bin/python
import argparse
from subprocess import call
import re
import os

default_neo4j_url = 'http://neo4j.com/artifact.php?name=neo4j-community-2.1.6-unix.tar.gz'
p_tar = re.compile('(http://neo4j.com/)(.*)')
p_dir = re.compile('(.*)\?name=(.*)(-unix\.tar\.gz)')


def download_and_extract(url):
    tar_path = p_tar.match(url).group(2)
    dir_path = p_dir.match(url).group(2)
    if not os.path.exists(tar_path):
        call(['wget', url])
    if not os.path.exists(dir_path):
        call(['tar', '-zxf', url.split('/')[-1]])


def start_neo4j(url):
    dir_path = p_dir.match(url).group(2)
    binary = os.path.join(dir_path, 'bin', 'neo4j')
    call([binary, 'start'])

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, action="store",
                        default=default_neo4j_url, help="neo4j source url")

    args = parser.parse_args()
    download_and_extract(args.url)
    start_neo4j(args.url)
