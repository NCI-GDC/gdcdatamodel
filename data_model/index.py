import requests
import json

def append_cypher(batch, query):
    batch.append({"statement": query})

def submit(batch):
    data = {"statements": batch}        
    print "Batch request for {0} statements".format(len(batch))
    r = requests.post('http://localhost:7474/db/data/transaction/commit', data=json.dumps(data))
    if r.status_code != 200: 
        raise Exception("Batch request for {0} statements failed: ".format(len(batch)) + r.text)
    return r.text

def get_types():
    batch = []
    append_cypher(batch, 'match n return DISTINCT n._type')
    return submit(batch)

def create_indices():
    types = json.loads(get_types())
    data = types['results'][0]['data']
    rows = [item['row'][0] for item in data]

    print 'types', rows
    batch = []
    for row in rows:
        if row is None: continue
        append_cypher(batch, 'CREATE INDEX ON :{_type}({key})'.format(_type=row, key='id'))
        append_cypher(batch, 'CREATE INDEX ON :{_type}({key})'.format(_type=row, key='_type'))

    print submit(batch)
        
if __name__ == '__main__':
    create_indices()
