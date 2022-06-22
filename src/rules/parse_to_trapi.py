import json

def add_node(t,node):
    if node == 'e0':
        return
    if node == 'e1':
        return
    if node in t['query_graph']['nodes']:
        return
    t['query_graph']['nodes'][node] = {'categories': ['biolink:NamedThing']}

def translate_nn(nn):
    if nn == 'e0':
        return '$chemical'
    if nn == 'e1':
        return '$disease'
    return nn

def add_edge(trapi,n0,n1,predicate,n):
    subject=translate_nn(n0)
    object = translate_nn(n1)
    trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicates':[predicate]}

def rule_to_trapi(rule):
    """Amie rules look like biolink:treats(e0,e1):- biolink:affects_abundance_of(e0,e2), biolink:has_phenotype(e2,e1)
    e0: drug node
    e1: disease node
    e2: new node"""
    t = {'query_graph': {'nodes': {'$chemical':{'categories':['biolink:ChemicalEntity']},
                                   '$disease':{'ids':['$disease_id']}}, 'edges': {}}}
    x = rule.split(':- ')[1]
    a_edges = x.split(', ')
    for ecount,amie_edge in enumerate(a_edges):
        predicate = f'{amie_edge[:-7]}'
        n0 = amie_edge[-6:-4]
        n1 = amie_edge[-3:-1]
        add_node(t,n0)
        add_node(t,n1)
        add_edge(t,n0,n1,predicate,ecount)
    return t

def go():
    with open('rules.py','w') as outf:
        outf.write('from string import Template\n')
        outf.write('rules = [\n')
        add_rules('2hops_new.txt',outf)
        add_rules('3hops_old.txt',outf)
        outf.write(']\n')

def add_rules(rulename,outf):
    with open(rulename, 'r') as inf:
        #h = inf.readline()
        for line in inf:
            if line.startswith('#'):
                continue
            x = line.split('\t')[0][:-1] #remove .
            trapi = rule_to_trapi(x)
            tstring = json.dumps(trapi)
            outf.write(f"Template('{tstring}'),\n")

if __name__ == '__main__':
    go()
