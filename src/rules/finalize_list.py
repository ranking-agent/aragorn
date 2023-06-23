import ast
import json

with open('kara_typed_rules/rules_with_types_cleaned.json','r') as inf:
    rules = json.load(inf)

for pred, rule_list in rules.items():
    rulenums = {}
    predj = json.loads(pred)
    name = predj["predicate"].split(":")[-1]
    if "qualifier_constraints" in predj:
        qset = predj["qualifier_constraints"][0]["qualifier_set"]
        qval = "_".join([q["qualifier_value"] for q in qset])
        name+=f"_{qval}"
    new_rule_list = []
    with open(f'kara_typed_rules/{name}.tsv','r') as inf:
        keep = set()
        for line in inf:
            x = line.strip().split("\t")
            rule_id = int(x[0])
            rule = x[1]
            filter = x[2]
            equivs = ast.literal_eval(x[3])
            subclass_of = ast.literal_eval(x[4])
            superclass_of = ast.literal_eval(x[5])
            if filter=="True":
                continue
            if len(subclass_of) > 0:
                continue
            eqs = [rule_id]+equivs
            if not min(eqs) == rule_id:
                continue
            keep.add(rule)
    print(len(keep))
    with open(f'kara_typed_rules/{name}_finalized.tsv', 'w') as outf:
        for rule in rule_list:
            if rule["Rule"] in keep:
                new_rule_list.append(rule)
                outf.write(f"{rule['Rule']}\n")
    rules[pred] = new_rule_list

with open('kara_typed_rules/rules_with_types_cleaned_finalized.json','w') as outf:
    json.dump(rules, outf, indent=4)