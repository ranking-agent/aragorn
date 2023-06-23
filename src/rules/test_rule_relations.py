import pytest
import json
from print_rules import compare

def get_rule(rule,predicate):
    with open("kara_typed_rules/rules_with_types_cleaned.json", "r") as f:
        allrules = json.load(f)
    rules = allrules[predicate]
    for r in rules:
        if r["Rule"] == rule:
            return r
def test_expression_abundance():
    decreased_expression = "?a  biolink:ameliorates  ?b  ?h  biolink:genetic_association  ?b  ?a  decreased_expression_biolink:affects  ?h   => ?a  biolink:treats  ?b"
    decreased_abundance = "?a  biolink:ameliorates  ?b  ?h  biolink:genetic_association  ?b  ?a  decreased_abundance_biolink:affects  ?h   => ?a  biolink:treats  ?b"
    decreased_expression_rule = get_rule(decreased_expression,"{\"predicate\": \"biolink:treats\"}")
    decreased_abundance_rule = get_rule(decreased_abundance,"{\"predicate\": \"biolink:treats\"}")
    assert decreased_expression_rule is not None
    assert decreased_abundance_rule is not None
    x = compare(decreased_expression_rule, decreased_abundance_rule)
    assert x == "subrule"

def test_expression_abundance_affects():
    decreased_expression = "?a  biolink:ameliorates  ?b  ?h  biolink:genetic_association  ?b  ?a  decreased_expression_biolink:affects  ?h   => ?a  biolink:treats  ?b"
    decreased_abundance = "?a  biolink:ameliorates  ?b  ?h  biolink:genetic_association  ?b  ?a  decreased_abundance_biolink:affects  ?h   => ?a  biolink:treats  ?b"
    affects_abundance = "?g  abundance_biolink:affects  ?a  ?a  biolink:ameliorates  ?b  ?g  biolink:genetic_association  ?b   => ?a  biolink:treats  ?b"
    decreased_expression_rule = get_rule(decreased_expression,"{\"predicate\": \"biolink:treats\"}")
    decreased_abundance_rule = get_rule(decreased_abundance,"{\"predicate\": \"biolink:treats\"}")
    affects_abundance_rule = get_rule(affects_abundance,"{\"predicate\": \"biolink:treats\"}")
    assert decreased_expression_rule is not None
    assert decreased_abundance_rule is not None
    x = compare(affects_abundance_rule, decreased_abundance_rule)
    assert x == "superrule"
    x = compare(affects_abundance_rule, decreased_expression_rule)
    assert x == "superrule"
