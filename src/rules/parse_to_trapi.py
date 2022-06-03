import json

def add_node(t,node):
    if node == 'a':
        return
    if node == 'b':
        return
    if node in t['query_graph']['nodes']:
        return
    t['query_graph']['nodes'][node] = {'categories': ['biolink:NamedThing']}

def translate_nn(nn):
    if nn == 'a':
        return '$chemical'
    if nn == 'b':
        return '$disease'
    return nn

def add_edge(trapi,n0,n1,predicate,n):
    subject=translate_nn(n0)
    object = translate_nn(n1)
    trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicate':predicate}

def rule_to_trapi(rule):
    """Amie rules look like treats(a,b):- affects_abundance_of(e,a), has_phenotype(e,b)
    a: drug node
    b: disease node
    e: new node"""
    t = {'query_graph': {'nodes': {'$chemical':{'categories':['biolink:ChemicalEntity']},
                                   '$disease':{'ids':['$disease_id']}}, 'edges': {}}}
    x = rule.split(':- ')[1]
    a_edges = x.split(', ')
    for ecount,amie_edge in enumerate(a_edges):
        predicate = f'biolink:{amie_edge[:-5]}'
        n0 = amie_edge[-4]
        n1 = amie_edge[-2]
        add_node(t,n0)
        add_node(t,n1)
        add_edge(t,n0,n1,predicate,ecount)
    return t

with open('amie.1.txt','r') as inf, open('rules.py','w') as outf:
    h = inf.readline()
    outf.write('from string import Template\n')
    outf.write('rules = [\n')
    for line in inf:
        x = line.split('\t')[0][1:-2] #strip quotes and a .
        trapi = rule_to_trapi(x)
        tstring = json.dumps(trapi)
        outf.write(f"Template('{tstring}'),\n")
    outf.write(']\n')
