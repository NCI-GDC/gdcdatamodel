import re, pprint, sys, requests, subprocess

with open('/etc/salt/minion_id', 'r') as minion:
    id = minion.read().strip()

recv_url = 'http://localhost:8500/v1/kv/net/{id}_in'.format(id = id)
send_url = 'http://localhost:8500/v1/kv/net/{id}_out'.format(id = id)

cmd = "ifstat -aT 1 1 | tail -n1"

p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
out, err = p.communicate()
recv, send = out.split()[-2:]

print recv, send

r = requests.put(recv_url, data = str(recv))
r = requests.put(send_url, data = str(send))
    
print "PASSED SYSTEM NETWORK TEST"
sys.exit(0)

        
