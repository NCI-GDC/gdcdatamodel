import re, pprint, sys, requests, subprocess

memPath='/proc/meminfo'

with open('/etc/salt/minion_id', 'r') as minion:
    id = minion.read().strip()

url = 'http://localhost:8500/v1/kv/cpu/{id}'.format(id = id)

cmd = "top -b -n2 | grep '^%Cpu' | tail -n 1 | gawk '{print $2+$4+$6}'"

p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
out, err = p.communicate()

usage = float(out.strip())

r = requests.put(url, data = str(usage))
    
print "PASSED SYSTEM CPU TEST"
sys.exit(0)

        
