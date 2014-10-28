import requests
import json
import requests
import sys
from pprint import pprint

# host = 'localhost'
host = '172.16.128.74'
path = '/home/ubuntu/tcga_dcc_download_{protection}.txt'
signpost = 'http://172.16.128.85/v0/'

def get_archive_map():
    batch = []
    archive_map = {}
    append_cypher(batch, 'match (n:file) where length(n.archive_name) > 0 return n')
    archives = submit(batch)['results'][0]['data']
    for row in archives:
        archive = row['row'][0]
        archive_map[archive['archive_name']] = archive['id']
    return archive_map

def post_did(protection, name, did, urls):

    acls = ['phs000178'] if protection == 'protected' else []

    data = {"acls": acls, "did": did, "urls": urls}
    pprint(data)

    r = requests.put(signpost, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    print r.text

def merge_tcga_dcc(protection):
    archive_map = get_archive_map()
    with open(path.format(protection=protection), 'r') as f:
        archives = f.read().strip().split('\n')

    error_count = 0
    for archive in archives:
        name = archive.replace('.tar', '').replace('.gz', '')
        did = archive_map.pop(name, None)
        if not did:
            error_count += 1
            print "Missing archive file: ", name
        else:
            url = 'swift://rados-bionimbus-pdc.opensciencedatacloud.org/tcga_dcc_download_{protection}/{name}'
            url = url.format(protection=protection, name=name)
            post_did(protection, archive, did, [url])

    # for archive, did in archive_map.items():
    #     post_did(protection, archive, did, [])
        
    print "Missing: ", error_count
    print "Extra: ", len(archive_map)

def submit(batch):
    data = {"statements": batch}        
    print "Batch request for {0} statements".format(len(batch))
    r = requests.post('http://{0}:7474/db/data/transaction/commit'.format(host), data=json.dumps(data))
    if r.status_code != 200: 
        raise Exception("Batch request for {0} statements failed: ".format(len(batch)) + r.text)
    return r.json()
def append_cypher(batch, query): batch.append({"statement": query})


if __name__ == '__main__':
    merge_tcga_dcc('protected')
    merge_tcga_dcc('public')
