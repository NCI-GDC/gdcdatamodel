import argparse
import re
from lxml import etree
import pprint
import requests
import json
import traceback

bio_url = 'https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/{lower_project}/bcr/nationwidechildrens.org/bio/clin/nationwidechildrens.org_{project}.bio.Level_1.{batch}.{version}.0/nationwidechildrens.org_biospecimen.{barcode}.xml'

u4reg = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
bcp = re.compile('(TCGA-..-....)-.*')
results = {}


def lookup_uuid(uuid):
    url = 'https://tcga-data.nci.nih.gov/uuid/uuidws/metadata/xml/uuid/{}'
    r = requests.get(url.format(uuid))
    r.encoding = 'UTF-8'
    return r.text


def load_uuids(path):
    p = re.compile('(.*Missing.*)({})'.format(u4reg))
    with open(path, 'r') as f:
        logfile = f.read().strip().split('\n')
    for line in logfile:
        match = p.match(line)
        if match:
            yield match.group(2)


def get_url(uuid, r, version):
    return bio_url.format(
        lower_project=r['project'].lower(),
        project=r['project'],
        batch=r['batch'],
        barcode=bcp.match(r['barcode']).group(1),
        version=version,
    )


def get_latest_version(uuid, r):
    basep = re.compile('(https://.*/bio/clin/)(.*)')
    hrefp = re.compile('(href=")(.*)(">.*)')
    vp = re.compile('.*Level_1\.{batch}\.(\d*).*'.format(batch=r['batch']))
    url = get_url(uuid, r, 50)
    base = basep.match(url).group(1)
    versions = [l for l in requests.get(base).text.strip().split() if
                '1.{}'.format(r['batch']) in l and '.tar.gz' not in l]
    latest = None
    if not len(versions):
        print 'No versions found!'
    for v in versions[-1:0:-1]:
        end = hrefp.match(v).group(2)
        archive = base + end
        xml_url = hrefp.match(
            [l for l in requests.get(archive).text.strip().split() if
             bcp.match(r['barcode']).group(1) in l][0]).group(2)
        final = archive + xml_url
        v_no = vp.match(final).group(1)
        if not latest:
            latest = v_no
        print final
        if uuid in requests.get(final).text:
            r['latest_existing_version'] = vp.match(final).group(1)
            r['latest_version'] = latest
            print ('Found in version {}'.format(v_no))
            return
    r['latest_version'] = latest


def parse_result(uuid, xml):
    print 'Working on {}'.format(uuid)
    results[uuid] = {}
    r = results[uuid]
    root = etree.fromstring(str(xml)).getroottree()
    p = re.compile('(.*)({})'.format(u4reg))
    r['analyte'] = p.match(root.xpath('//analyte')[0].
                           attrib['href']).group(2)
    r['participant'] = p.match(root.xpath('//participant')[0].
                               attrib['href']).group(2)
    r['portion'] = p.match(root.xpath('//portion')[0].
                           attrib['href']).group(2)
    r['sample'] = p.match(root.xpath('//sample')[0].
                          attrib['href']).group(2)
    r['project'] = root.xpath('//disease/abbreviation')[0].text
    r['barcode'] = root.xpath('//barcodes/barcode')[0].text
    r['batch'] = root.xpath('//batch')[0].text
    r['redacted'] = root.xpath('//redacted')[0].text
    r['latest_existing_version'] = None
    get_latest_version(uuid, r)


def find_versions(uuids):
    for uuid in uuids:
        xml = lookup_uuid(uuid)
        try:
            parse_result(uuid, xml)
        except Exception, msg:
            print(msg)
            traceback.print_exc()
            results[uuid] = 'FAILED TO PARSE UUID'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, required=True,
                        help='log file to parse')
    parser.add_argument('-o', '--output', default='missing.json',
                        type=str, help='log file to parse')
    args = parser.parse_args()
    uuids = set(list(load_uuids(args.file)))
    print 'Found {} distinct uuids'.format(len(uuids))
    find_versions(uuids)

    pprint.pprint(results)
    with open(args.output, 'w') as f:
        f.write(json.dumps(results, indent=2))
