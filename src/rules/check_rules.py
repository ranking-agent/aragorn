import redis
import json
import os
import requests

def get_redis(db=1):
    r = redis.Redis(host='localhost', port=6379, db=db)
    return r

def get_nres(m):
    message = m.get('message',{})
    results = message.get('results',[])
    return len(results)

r = get_redis()
ks = r.keys()
print(f'{len(ks)} results')

identifiers = [ k.decode() for k in ks ]
nodenorm_url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/1.2/")}get_normalized_nodes'
nnp = {"curies": identifiers, "conflate": True}
nnr = requests.post(nodenorm_url,json=nnp).json()
labels = { x:(y['id'].get('label','?') if y is not None else '?') for x,y in nnr.items() }

with open('numresults.txt','w') as outf:
    outf.write('id\tname\tnum_cached_results\n')
    for k in identifiers:
        s = r.get(k).decode()
        message = json.loads(s)
        n = get_nres(message)
        outf.write(f'{k}\t{labels[k]}\t{n}\n')
