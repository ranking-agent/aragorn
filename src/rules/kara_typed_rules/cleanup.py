import json
#Note, this was mostly written with github copilot, by creating comments and letting it produce all the code.
# Some notes - to make it work, I had to define the entire json structure in the comments, otherwise it would look
# for the "categories" at the top level all the time.
# It wrote get_preferred category reasonably well, but it unjustly assumed that if there was not a category in the preferred list,
# then it should return the first cateogry in the list.  I changed it to raise an exception instead, since otherwise
# it's effectively a silent fail.
# Also, the way of hunting for preferred categories will be potentially slow, though not at this scale to be fair.
# Finally, I tried writing a version that didn't assume the structure of the json, by writing in the comment things
# like "don't assume the structure of the json" but it was completely stuck on its already written function and
# wouldn't produce anything else.

# Read a JSON file "rules_with_types.json" and produce a new JSON file "rules_with_types_cleaned.json"
# in the new file, whenever there is a "categories" element at any depth in the structure,
# make sure that it only has one element in the list.
# If there is more than one element, remove all elements except for the preferred one.
# Preference is dictated by the following order:
# ['biolink:DiseaseOrPhenotypicFeature', 'biolink:Disease', 'biolink:Gene', 'biolink:Protein', 'biolink:SmallMolecule',
# 'biolink:ChemicalEntity', 'biolink:BiologicalProcessOrActivity', 'biolink:GeneOrGeneProduct', 'biolink:BiologicalEntity']

def get_preferred_category(categories):
    """
    Given a list of categories, return the preferred category
    if there is no preferred category, raise an exception
    :param categories:
    :return:
    """
    preferred_categories = ['biolink:DiseaseOrPhenotypicFeature', 'biolink:Disease', 'biolink:Gene', 'biolink:Protein', 'biolink:SmallMolecule',
                            'biolink:BiologicalProcess', 'biolink:ChemicalEntity', 'biolink:BiologicalProcessOrActivity', 'biolink:GeneOrGeneProduct', 'biolink:BiologicalEntity']
    for preferred_category in preferred_categories:
        if preferred_category in categories:
            return preferred_category
    raise Exception(f"no preferred category found in {categories}")

def clean_rules_with_types():
    """
    Read a JSON file "rules_with_types.json" and produce a new JSON file "rules_with_types_cleaned.json"
    The structure of both files should be the same:
    a map between a rulesetname and a list of rules.
    Each rule is a map. One key is template, which has a map as a value
    one key of that map is "query_graph", which has a map as a value
    one key of that map is "nodes" which has a list as a value
    each node in the list is a map, from a nodename to another map, one key of which is "categories" which has a list as a value
    in the new file make sure that each node only has a single entry in the categories list.
    If there is more than one element, remove all elements except for the preferred one.
    Preference is dictated by the following order:
    ['biolink:DiseaseOrPhenotypicFeature', 'biolink:Disease', 'biolink:Gene', 'biolink:Protein', 'biolink:SmallMolecule',
    'biolink:GeneOrGeneProduct', 'biolink:BiologicalEntity']
    If there is only a single element, leave it alone.
    :return:
    """
    with open("rules_with_types.json", "r") as f:
        rules_with_types = json.load(f)
    for rulesetname, rules in rules_with_types.items():
        for rule in rules:
            query_graph = rule["template"]["query_graph"]
            for nodename, node in query_graph["nodes"].items():
                categories = node["categories"]
                if len(categories) > 1:
                    preferred_category = get_preferred_category(categories)
                    node["categories"] = [preferred_category]
    with open("rules_with_types_cleaned.json", "w") as f:
        json.dump(rules_with_types, f, indent=2)

if __name__ == "__main__":
    clean_rules_with_types()