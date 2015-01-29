#!/user/bin/python
import argparse
from subprocess import check_call
import re
import os

default_neo4j_url = 'http://neo4j.com/artifact.php?name=neo4j-community-2.1.6-unix.tar.gz'
p_tar = re.compile('(http://neo4j.com/)(.*)')
p_dir = re.compile('(.*)\?name=(.*)(-unix\.tar\.gz)')
neo_config_additions = """
# Mount the load2neo extension at /load2neo
org.neo4j.server.thirdparty_jaxrs_classes=com.nigelsmall.load2neo=/load2neo
"""


def download_and_extract(url):
    tar_path = p_tar.match(url).group(2)
    dir_path = p_dir.match(url).group(2)
    if not os.path.exists(tar_path):
        check_call(['wget', url])
    if not os.path.exists(dir_path):
        check_call(['tar', '-zxf', url.split('/')[-1]])


def restart_neo4j(url):
    dir_path = p_dir.match(url).group(2)
    binary = os.path.join(dir_path, 'bin', 'neo4j')
    check_call([binary, 'restart'])


def install_load2neo(url):
    neo_dir = p_dir.match(url).group(2)
    conf_path = os.path.join(neo_dir, 'conf', 'neo4j-server.properties')
    with open(conf_path, 'a') as conf:
        conf.write(neo_config_additions)
    bin_dir = os.path.dirname(os.path.realpath(__file__))
    cp_cmd = 'cp {}/*.jar {}/'.format(
        os.path.join(bin_dir, 'neo4j_config'),
        os.path.join(neo_dir, 'plugins'))
    check_call(cp_cmd, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, action="store",
                        default=default_neo4j_url, help="neo4j source url")
    parser.add_argument("--no-geoff", action="store_true",
                        help="install load2neo plugin for geoff import")
    args = parser.parse_args()
    download_and_extract(args.url)
    if not args.no_geoff:
        install_load2neo(args.url)
    restart_neo4j(args.url)
