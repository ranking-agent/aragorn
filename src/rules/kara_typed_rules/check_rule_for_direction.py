import json

with open("rules_with_types_cleaned_finalized.json","r") as infile:
    rules = json.load(infile)

for key,ruleset in rules.items():
    print(key)
    for rule in ruleset:
        trapi = rule["template"]
        edges = trapi["query_graph"]["edges"]
        for edge_id,edge in edges.items():
            if edge["predicates"][0] == "biolink:affects":
                try:
                    qualifiers = edge["qualifier_constraints"][0]["qualifier_set"]
                    found = False
                    for qualifier in qualifiers:
                        if qualifier["qualifier_type_id"] == "biolink:object_direction_qualifier":
                            found = True
                    if not found:
                        print(" ",rule["Rule"])
                except:
                    print(" ", rule["Rule"])