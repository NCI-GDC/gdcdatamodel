import re, pprint, sys, requests

memPath='/proc/meminfo'

with open('/etc/salt/minion_id', 'r') as minion:
    id = minion.read().strip()

url = 'http://localhost:8500/v1/kv/memory/{id}'.format(id = id)


with open(memPath, 'r') as memf:
    memory = memf.read().split('\n')

p = re.compile('(.+):( )+(\d+)( )+(.*)')

values = {}
for mem in memory:
    match = p.match(mem)
    if match is not None:
        name = match.group(1)
        number = int(match.group(3))
        unit = match.group(5)
        values[name] = [number, unit]

pprint.pprint(values)
free = float(values['MemFree'][0])
total = float(values['MemTotal'][0])
usage = 1 - free/total
print url

r = requests.put(url, data = str(usage))

if free < 1000:
    print "LOW FREE MEMORY"
    sys.exit(1)
elif total < 100:
    print "FREE MEMORY TEST FAILED"
    sys.exit(2)
    
print "PASSED SYSTEM MEMORY TEST"
sys.exit(0)

        
