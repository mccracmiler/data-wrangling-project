#Explore element types
'''
This code will explore a given "tag" k value, count each unique
value and print a table of the values so that the user can determine 
if a particular field should be cleaned
'''
# -*- coding: utf-8 -*-
import pandas as pd
import xml.etree.cElementTree as ET
from collections import defaultdict
import re

osm_file = open("alamedamap.osm", "r")
element_type_re = re.compile(r'\S+\.?$', re.IGNORECASE)
element_types = defaultdict(int)

def audit_element_type(element_types, element_name):
    m = element_type_re.search(element_name) 
    if m:
        element_type = element_name
        element_types[element_type] += 1

def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print "%s: %d" % (k, v) 

def is_element_name(elem):
    return (elem.tag == "tag") 

def audit():
    for event, elem in ET.iterparse(osm_file):
        if is_element_name(elem):
            audit_element_type(element_types, elem.attrib['k'])
    print_sorted_dict(element_types)        
    elementtypes = pd.DataFrame.from_dict(element_types, orient='index')
    elementtypes.to_csv("elementKtypes.csv",encoding='utf-8')    
    print "Number of tags: ", len(element_types) 
if __name__ == '__main__':
    audit()