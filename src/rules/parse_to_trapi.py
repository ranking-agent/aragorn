import json
import requests
from copy import deepcopy

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

def get_qualified_preds():
    bads=['biolink:increases_molecular_modification_of',
    'biolink:decreases_molecular_modification_of',
    'biolink:affects_degradation_of' ,
    'biolink:increases_degradation_of',
    'biolink:decreases_degradation_of',
    'biolink:affects_stability_of',
    'biolink:decreases_stability_of',
    'biolink:increases_molecular_interaction',
    'biolink:decreases_response_to',
    'biolink:increases_uptake_of',
    'biolink:affects_abundance_of',
    'biolink:increases_splicing_of',
    'biolink:expression_decreased_by',
    'biolink:expression_increased_by',
    'biolink:increases_response_to',
    'biolink:increases_localization_of',
    'biolink:decreases_localization_of',
    'biolink:increases_metabolic_processing_of',
    'biolink:decreases_metabolic_processing_of',
    'biolink:increases_synthesis_of',
    'biolink:affects_transport_of',
    'biolink:decreases_secretion_of',
    'biolink:affects_activity_of',
    'biolink:increases_activity_of']
    bl_url = 'https://bl-lookup-sri.renci.org/resolve_predicate?version=v3.1.1&predicate='
    #bl_url = 'http://0.0.0.0:8144/resolve_predicate?version=v3.0.3&predicate='
    translation = {}
    for b in bads:
        ctd_bad = f"CTD:{b.split(':')[1]}"
        resp = requests.get(bl_url+ctd_bad)
        if resp.status_code != 200:
            print('bad status',b,resp.status_code)
            print(bl_url)
            exit()
        else:
            j = resp.json()
            translation[b] = j
    return translation

def add_edge(trapi,n0,n1,predicate,n,qpredmap):
    subject=translate_nn(n0)
    object = translate_nn(n1)
    if predicate in qpredmap:
        qpp = qpredmap[predicate]
        op = next(iter(qpp))
        props = deepcopy(qpp[op])
        props.pop('inverted',None)
        props.pop('label',None)
        props.pop('qualified_predicate',None)
        if 'predicate' not in props:
            print('wtf')
        trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicates': [props['predicate']]}
        props.pop('predicate',None)
        qset = []
        for k,v in props.items():
            qset.append( {'qualifier_type_id':k, 'qualifier_value': v})
        trapi['query_graph']['edges'][f'edge_{n}']['qualifier_constraints']=[{'qualifier_set':qset}]
    else:
        trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicates':[predicate]}

def rule_to_trapi(rule,qpredmap):
    """Amie rules look like biolink:treats(e0,e1):- biolink:affects_abundance_of(e0,e2), biolink:has_phenotype(e2,e1)
    e0: drug node
    e1: disease node
    e2: new node"""
    #Note that we're putting a category on the disease node.  This is to make the cypher index happy.  It should not be
    # required in future versions of the transpiler
    t = {'query_graph': {'nodes': {'$chemical':{'categories':['biolink:ChemicalEntity']},
                                   '$disease':{'ids':['$disease_id'], 'categories':['biolink:DiseaseOrPhenotypicFeature']}}, 'edges': {}}}
    x = rule.split(':- ')[1]
    a_edges = x.split(', ')
    for ecount,amie_edge in enumerate(a_edges):
        predicate = f'{amie_edge[:-7]}'
        n0 = amie_edge[-6:-4]
        n1 = amie_edge[-3:-1]
        add_node(t,n0)
        add_node(t,n1)
        add_edge(t,n0,n1,predicate,ecount,qpredmap)
    return t

def go():
    with open('rules.py','w') as outf:
        outf.write('from string import Template\n')
        outf.write('rules = [\n')
        add_rules('2hops_new.txt',outf)
        add_rules('3hops_old.txt',outf,maxrules=50)
        outf.write(']\n')

def add_rules(rulename,outf,maxrules=9999999):
    qualified_predicate_map = get_qualified_preds()
    with open(rulename, 'r') as inf:
        #h = inf.readline()
        nwritten = 0
        for line in inf:
            if line.startswith('#'):
                continue
            x = line.split('\t')[0][:-1] #remove .
            trapi = rule_to_trapi(x,qualified_predicate_map)
            tstring = json.dumps(trapi)
            outf.write(f"Template('{tstring}'),\n")
            nwritten +=1
            if nwritten >= maxrules:
                break

if __name__ == '__main__':
    #This converts rules derived under biolink2 into biolink3 queries.  When we get biolink3 rules
    # we won't have to do this exact thing, but it will change again...
    go()
