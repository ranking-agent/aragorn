import json
from collections import defaultdict
from itertools import combinations
from bmt import Toolkit
tk= Toolkit()

# Read kara_typed_rules/rules_with_types_cleaned.json
# For each predicate, print all the rules, one per line into a file

def get_next_node(template,used_edges, prev_node):
    for edge_id, edge in template["query_graph"]["edges"].items():
        if edge_id in used_edges:
            continue
        else:
            if edge["subject"] == prev_node:
                used_edges.add(edge_id)
                return edge["object"]
            if edge["object"] == prev_node:
                used_edges.add(edge_id)
                return edge["subject"]
    print("Error: no next node found")
    exit()

def linear_node_map(type_to_nodes1, type_to_nodes2, template1, template2):
    node_map = {"$source": "$source"}
    next_node_1 = "$source"
    next_node_2 = "$source"
    used1 = set()
    used2 = set()
    while next_node_1 != "$target":
        next_node_1 = get_next_node(template1, used1, next_node_1)
        next_node_2 = get_next_node(template2, used2, next_node_2)
        node_map[next_node_1] = next_node_2
    return node_map


def create_node_map(template1, template2):
    node_map = {"$source": "$source", "$target": "$target"}
    nodes1 = template1["query_graph"]["nodes"]
    nodes2 = template2["query_graph"]["nodes"]
    #Can we distinguish nodes by type?
    type_to_nodes1 = defaultdict(list)
    type_to_nodes2 = defaultdict(list)
    for nodes,ttn in [(nodes1,type_to_nodes1),(nodes2,type_to_nodes2)]:
        for node in nodes:
            if node == "$source" or node == "$target":
                continue
            ntype = nodes[node]["categories"][0]
            ttn[ntype].append(node)
    if len(type_to_nodes1) != len(type_to_nodes2):
        # these are different rules
        return None
    multi = False
    for ntype in type_to_nodes1:
        if len(type_to_nodes1[ntype]) != len(type_to_nodes2[ntype]):
            # these are different rules
            return None
        if len(type_to_nodes1[ntype]) > 1:
            return linear_node_map(type_to_nodes1,type_to_nodes2,template1, template2)
        for n1,n2 in zip(type_to_nodes1[ntype],type_to_nodes2[ntype]):
            node_map[n1] = n2
    return node_map

def create_edge_map(template1, template2, node_map):
    try:
        edges_1 = {}
        edges_2 = {}
        for template,edges in [(template1,edges_1), (template2,edges_2)]:
            for edge_id, edge in template["query_graph"]["edges"].items():
                ekey = (edge["subject"], edge["object"])
                edges[ekey] = edge_id
        #Check the topology
        inverse_nodemap = {v:k for k,v in node_map.items()}
        topcount_1 = defaultdict(int)
        topcount_2 = defaultdict(int)
        for nodes,edge in edges_1.items():
            key = frozenset(nodes)
            topcount_1[key] += 1
        for nodes,edge in edges_2.items():
            key = frozenset([inverse_nodemap[n] for n in nodes])
            topcount_2[key] += 1
        if topcount_1 != topcount_2:
            return None
        #topology matches, proceed
        edge_map = {}
        for (n1,n2),e1 in edges_1.items():
            ekey2 = (node_map[n1], node_map[n2])
            if ekey2 in edges_2:
                e2 = edges_2[ekey2]
                edge_map[e1] = (e2,False)
            else:
                ekey3 = (node_map[n2], node_map[n1])
                if ekey3 in edges_2:
                    e2 = edges_2[ekey3]
                    edge_map[e1] = (e2, True)
                else:
                    return None
        return edge_map
    except:
        print("yuck")

def simple_edge(edge):
    simple_edge = {"predicate": edge["predicates"][0], "qualifiers": {}}
    if "qualifier_constraints" in edge:
        qset = edge["qualifier_constraints"][0]["qualifier_set"]
        for q in qset:
            simple_edge["qualifiers"][q["qualifier_type_id"]] = q["qualifier_value"]
    return simple_edge

def sub_predicates(p):
    return [tk.get_element(x).slot_uri for x in tk.get_children(p)]

def cmp_predicate(pred1, pred2, quals1, quals2):
    """ if pred1 is a subclass of pred2, return "subrule"
        if pred2 is a subclass of pred1, return "superrule"
        if pred1 and pred2 are equivalent, return "equivalent"
        if pred1 and pred2 are different, return "different"
    """
    #This condition shouldn't happen but just in case..
    if pred1 == pred2:
        return "equivalent"
    if pred2 in sub_predicates(pred1) and len(quals1) == 0:
        return "superrule"
    if pred1 in sub_predicates(pred2) and len(quals2) == 0:
        return "subrule"
    return "different"

def cmp_quals(edge1,edge2):
    subquals = {"abundance": ["expression","synthesis"],"decreased":[], "increased": [],
                "activity_or_abundance": ["activity","abundance","expression","synthesis"],
                "transport":["secretion", "uptake"], "expression":[], "synthesis":[], "secretion":[], "uptake":[],
                "molecular_interaction": [], "activity": [], "metabolic_processing": [], "degradation": [],
                "localization": [], "mutation_rate": [], "molecular_modification": [],
                "downregulated": [], "upregulated": []}
    qualnames = set(edge1["qualifiers"].keys()).union(set(edge2["qualifiers"].keys()))
    state = "equivalent"
    for q in qualnames:
        if q in edge1["qualifiers"] and q not in edge2["qualifiers"]:
            if state == "equivalent":
                state = "subrule"
            elif state == "superrule":
                return "different"
        elif q in edge2["qualifiers"] and q not in edge1["qualifiers"]:
            if state == "equivalent":
                state = "superrule"
            elif state == "subrule":
                return "different"
        elif edge1["qualifiers"][q] == edge2["qualifiers"][q]:
            continue
        else:
            if edge1["qualifiers"][q] in subquals and edge2["qualifiers"][q] in subquals[edge1["qualifiers"][q]]:
                if state == "equivalent":
                    state = "superrule"
                elif state == "subrule":
                    return "different"
            elif edge2["qualifiers"][q] in subquals and edge1["qualifiers"][q] in subquals[edge2["qualifiers"][q]]:
                if state == "equivalent":
                    state = "subrule"
                elif state == "superrule":
                    return "different"
            elif edge1["qualifiers"][q] in subquals and edge2["qualifiers"][q] in subquals:
                return "different"
            else:
                print(q, edge1["qualifiers"][q], edge2["qualifiers"][q])
                exit()
    return state


def cmp_edge(edge1, edge2):
    """Look at the predicate and qualifiers and determine if the edges are the same, different, or one is a subrule of the other."""
    #There may be a way to do this with BMT, but that's for later
    se1 = simple_edge(edge1)
    se2 = simple_edge(edge2)
    if se1 == se2:
        return "equivalent"
    #If the predicates are the same, compare the qualifiers
    if se1["predicate"] == se2["predicate"]:
        return cmp_quals(se1,se2)
    #if the predicates are different, one might be a subpredicate of the other
    # but only if the superpredicate is unqualified
    return cmp_predicate(se1["predicate"], se2["predicate"], se1.get("qualifiers",{}), se2.get("qualifiers",{}))

def is_symmetric(predicate):
    symmetric = (tk.get_element(predicate).symmetric is not None) and (tk.get_element(predicate).symmetric == True)
    return symmetric

def compare(rule1, rule2):
    # Compare 2 rules.
    # Options, rule 1 is a subrule or rule 2, return "subrule"
    # rule 2 is a subrule of rule 1, return "superrule"
    # rule 1 and rule 2 are the same given symmetry, return "symmetric"
    # rule 1 and rule 2 are different, return "different"
    template1 = rule1["template"]
    template2 = rule2["template"]
    if rule1["Rule"] == "?f  biolink:has_phenotype  ?b  ?b  biolink:has_phenotype  ?f  ?a  biolink:treats  ?f   => ?a  biolink:treats  ?b":
        if rule2["Rule"] == "?a  biolink:has_adverse_event  ?b  ?h  biolink:has_phenotype  ?b  ?a  biolink:treats  ?h   => ?a  biolink:treats  ?b":
            print("hi")
    # There are two possibilites we can handle: Linear chains or cases where the non-source/target nodes are different types
    # This lets us unambiguously map the nodes and edges across the rules.
    # First, see if the rules have the same number of nodes and edges
    if len(template1["query_graph"]["nodes"]) != len(template2["query_graph"]["nodes"]):
        return "different"
    if len(template1["query_graph"]["edges"]) != len(template2["query_graph"]["edges"]):
        return "different"
    node_map = create_node_map(template1,template2)
    edge_map = create_edge_map(template1,template2,node_map)
    if edge_map is None:
        return "different"
    state = "equivalent"
    for edge1, (edge2,inverted) in edge_map.items():
        ecmp = cmp_edge(template1["query_graph"]["edges"][edge1], template2["query_graph"]["edges"][edge2])
        if ecmp == "different":
            state = "different"
            break
        elif ecmp == "subrule":
            if state in ["equivalent","subrule"]:
                state = "subrule"
            elif state == "superrule":
                state = "different"
                break
        elif ecmp == "superrule":
            if state in ["equivalent","superrule"]:
                state = "superrule"
            elif state == "subrule":
                state = "different"
                break
        else:
            if inverted:
                if not is_symmetric(template1["query_graph"]["edges"][edge1]["predicates"][0]):
                    state = "different"
                    break
    return state


def filter_treats(rule_list):
    #This should probably be more careful. Maybe these should only be on a->b paths?
    good = []
    bad = []
    for rule in rule_list:
        simprule = rule["Rule"]
        if "causes" in simprule:
            bad.append(rule)
        elif "contributes_to" in simprule:
            bad.append(rule)
        elif "adverse_event" in simprule:
            bad.append(rule)
        else:
            good.append(rule)
    return good,bad

def filter_super_opposites(rule_list, pred):
    #If we want to predict a-[pred]->b and pred has an opposite, then we don't want
    # any rule to have any superclasses of a-[opposite]->b.
    # Having an opposite means that there is an object direction qualifier
    # I could write a bunch of biolinky stuff, but for now, I really just need to watch out for a few.
    bad_rules = []
    good_rules = []
    print(pred)
    bad_edges = [ {"predicate":"biolink:affects","qualifiers":{"biolink:object_aspect_qualifier": "activity"}},
                  {"predicate":"biolink:affects","qualifiers":{"biolink:object_aspect_qualifier": "activity_or_abundance"}},
                  {"predicate":"biolink:affects"}]
    for rule in rule_list:
        trapi = rule["template"]
        good = True
        for edge_id,edge in trapi["query_graph"]["edges"].items():
            if not good:
                continue
            if edge["subject"] == "$source" and edge["object"] == "$target":
                se = simple_edge(edge)
                if se in bad_edges:
                    bad_rules.append(rule)
                    good = False
        if good:
            good_rules.append(rule)
    if len(good_rules) + len(bad_rules) != len(rule_list):
        print("oops")
        print(len(good_rules), len(bad_rules), len(rule_list))
        exit()
    return good_rules,bad_rules

def apply_filters(rule_list, pred):
    #this is sort of crummy b/c we have to different filtering for different predicates
    if pred == {'predicate': "biolink:treats"}:
        return filter_treats(rule_list)
    elif pred == {'predicate': "biolink:contraindicated_for"}:
        return rule_list,[]
    else:
        return filter_super_opposites(rule_list,pred)

# Load the json
with open('kara_typed_rules/rules_with_types_cleaned.json','r') as inf:
    rules = json.load(inf)

for pred, rule_list in rules.items():
    rulenums = {}
    pred = json.loads(pred)
    name = pred["predicate"].split(":")[-1]
    if "qualifier_constraints" in pred:
        qset = pred["qualifier_constraints"][0]["qualifier_set"]
        qval = "_".join([q["qualifier_value"] for q in qset])
        name+=f"_{qval}"
    rulesort = defaultdict(list)
    good_rules,bad_rules = apply_filters(rule_list, pred)
    for rule in good_rules:
        trapi = rule["template"]
        othertypes = defaultdict(int)
        for nid, node in trapi["query_graph"]["nodes"].items():
            if not nid in ["$source","$target"]:
                othertypes[ node["categories"][0] ] += 1
        k = set()
        for ot, c in othertypes.items():
            k.add((ot,c))
        fs = frozenset(k)
        rulesort[fs].append(rule)
    rule_rel = defaultdict(lambda: {"filtered": False, "equivalent":[],"subrule_of":[],"superrule_of":[]})
    for rule in bad_rules:
        rulenums[rule["Rule"]] = len(rulenums)
        rule_rel[rulenums[rule["Rule"]]]["filtered"]=True
    with open(f'kara_typed_rules/{name}.tsv','w') as outf:
        for k, v in rulesort.items():
            #outf.write(f"{k},{len(v)}\n")
            #for rule in v:
            #    outf.write(f"{rule['Rule']}\n")
            for rule1, rule2 in combinations(v,2):
                if rule1["Rule"] not in rulenums:
                    rulenums[rule1["Rule"]] = len(rulenums)
                if rule2["Rule"] not in rulenums:
                    rulenums[rule2["Rule"]] = len(rulenums)
                rulenum1 = rulenums[rule1["Rule"]]
                rulenum2 = rulenums[rule2["Rule"]]
                x = compare(rule1,rule2)
                if x == "equivalent":
                    rule_rel[rulenum1]["equivalent"].append(rulenum2)
                    rule_rel[rulenum2]["equivalent"].append(rulenum1)
                elif x == "subrule":
                    rule_rel[rulenum1]["subrule_of"].append(rulenum2)
                    rule_rel[rulenum2]["superrule_of"].append(rulenum1)
                elif x == "superrule":
                    rule_rel[rulenum1]["superrule_of"].append(rulenum2)
                    rule_rel[rulenum2]["subrule_of"].append(rulenum1)
        inverted_rulenums = {v:k for k,v in rulenums.items()}
        for i in range(len(rulenums)):
            outf.write(f"{i}\t{inverted_rulenums[i]}\t{rule_rel[i]['filtered']}\t{rule_rel[i]['equivalent']}\t{rule_rel[i]['subrule_of']}\t{rule_rel[i]['superrule_of']}\n")
