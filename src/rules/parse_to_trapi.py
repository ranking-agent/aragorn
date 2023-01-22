import json
import requests
from copy import deepcopy

def add_node(t,node):
    if node in t['query_graph']['nodes']:
        return
    t['query_graph']['nodes'][node] = {'categories': ['biolink:NamedThing']}

def add_edge(trapi,subject,object,predicate,n,qpredmap):
    if predicate.startswith('biolink:'):
        trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicates':[predicate]}
    else:
        #Its a fake predicate that maps to a predicate/qualifier set
        qpp = qpredmap[predicate]
        biolink_predicate = qpp['predicate']
        qualifiers = qpp['qualifier_set']
        trapi['query_graph']['edges'][f'edge_{n}'] = {'subject': subject, 'object': object, 'predicates': [biolink_predicate]}
        qset = []
        qset = [{'qualifier_type_id':k, 'qualifier_value': v} for k,v in qualifiers.items()]
        trapi['query_graph']['edges'][f'edge_{n}']['qualifier_constraints']=[{'qualifier_set':qset}]

def rule_to_trapi(rule,rule_conf,qmap):
    #Note that we're putting a category on the disease node.  This is to make the cypher index happy.  It should not be
    # required in future versions of the transpiler
    t = {'query_graph': {
        'nodes': {'$source':{'ids':['$source_id'],'categories':[rule_conf["source"]]},
                  '$target':{'ids':['$target_id'], 'categories':[rule_conf["target"]]}},
        'edges': {}}}
    head_tail = rule.split('=> ')
    head = head_tail[0]
    tail_parts = head_tail[1].split()
    nodemap = {tail_parts[0]: "$source", tail_parts[2]: "$target"}
    rulesplit = head.split()
    ecount = 0
    #There are some rules like (x)-(source)-(target) => (source)-(target) that I want to get rid of
    source_ok = False
    target_ok = False
    for counter in range(0, len(rulesplit), 3):
        n0 = rulesplit[counter]
        predicate = rulesplit[counter+1]
        n1 = rulesplit[counter+2]
        if n0 in nodemap:
            n0 = nodemap[n0]
        else:
            n0 = n0[1:] #strip ?
        if n1 in nodemap:
            n1 = nodemap[n1]
        else:
            n1 = n1[1:]
        add_node(t,n0)
        add_node(t,n1)
        if (n0 == "$source") and (not n1 == "$target"):
            source_ok = True
        if (n1 == "$source") and (not n0 == "$target"):
            source_ok = True
        if (n0 == "$target") and (not n1 == "$source"):
            target_ok = True
        if (n1 == "$target") and (not n0 == "$source"):
            target_ok = True
        add_edge(t,n0,n1,predicate,ecount,qmap)
        ecount += 1
    if len(t['query_graph']['nodes']) == 2 or (source_ok and target_ok):
        return t
    else:
        print("Rejecting")
        print(rule)
        return None


def parse_line(line, header, source, rule_conf, qmap):
    line_parts = line.strip('\n').split('\t')
    if len(header) != len(line_parts):
        #It's probably some trailing info
        return {}
    rule = { h:p for h,p in zip(header,line_parts)}
    #Do we have enough positive examples?
    minpos = source.get('min_positive_example', 0)
    if int( rule["Positive Examples"] ) <= minpos:
        return {}
    # Do we have enough confidence?
    minconf = source.get('min_std_confidence', 0)
    if float(rule["Std Confidence"]) <= minconf:
        return {}
    # Is the rule going to bring back too many results?
    maxbody = source.get("max_body_size", 99999999999)
    if int(rule["Body size"]) >= maxbody:
        return {}
    template = rule_to_trapi(rule['Rule'],rule_conf,qmap)
    if template is not None:
        rule["template"] = template
        return rule
    return {}

def parse_rulefile(source, rule_conf, qmap):
    floc = f"AMIE_rules/{source['input_file']}"
    rules = []
    try:
        with open(floc,"r") as inf:
            line = inf.readline()
            while not line.startswith('Rule\t'):
                line = inf.readline()
            header = line.strip().split('\t')
            for colname in ["Rule", "Positive Examples", "Std Confidence"]:
                if colname not in header:
                    print(f"Missing column: {colname}")
                    return []
            for line in inf:
                rule = parse_line(line, header, source, rule_conf, qmap)
                if len(rule) > 0:
                    rules.append(rule)
    except FileNotFoundError:
        print(f'{floc}: No such file.')
        return []
    except ValueError:
        print(f"Can't find expected columns in {floc}")
    return rules

def sort_rules(rules,config):
    if len(rules) > 0:
        print(rules[0].keys())
    rules.sort(key=lambda x: x[config["sort"]])
    rules.reverse()

def chop_rules(rules,rule_config):
    return rules[:rule_config["keep"]]

def generate_key(rule_config):
    key = json.dumps(rule_config["edge_definition"],sort_keys=True)
    return key

def create_rule(rulename, rule_config, qmap):
    key = generate_key(rule_config)
    rules = []
    for source in rule_config['AMIE_inputs']:
        rules += parse_rulefile(source, rule_config, qmap)
    sort_rules(rules, rule_config)
    rules = chop_rules(rules, rule_config)
    return key,rules

def read_qmap():
    with open('qualifier_map.json','r') as inf:
        qmap = json.load(inf)
    #Kara included the biolink ones, but we don't need them
    bks = [ k for k in qmap.keys() if k.startswith('biolink') ]
    for bk in bks:
        del qmap[bk]
    #Also, let's remove any empty qualifier_set elements
    for k,v in qmap.items():
        if 'qualifier_set' in v and len(v['qualifier_set']) == 0:
            del v['qualifier_set']
    return qmap

def go():
    with open('rule_config.json','r') as inf:
        config = json.load(inf)
    qmap = read_qmap()
    all_rules = {}
    for rulename, rule_config in config.items():
        key,rules = create_rule(rulename, rule_config, qmap)
        if len(rules) > 0:
            all_rules[key] = rules
    with open('rules.json','w') as outf:
        json.dump(all_rules,outf,indent=4)

if __name__ == '__main__':
    go()
