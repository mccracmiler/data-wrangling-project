"""
This code prepares the data to be inserted into a SQL database.  The scrpit parses the elements in the 
OSM XML file, transforming them from document format to tabular format making it possible to write 
to .csv files to be into imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

## Shape Element Function
The function take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag is ignored
- if the tag "k" value contains a ":" the characters before the ":" are set as the tag type
  and characters after the ":" are be set as the tag key
- if there are additional ":" in the "k" value they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field just contains an empty list.

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes are ignored

The "way_tags" field hold a list of dictionaries, following the exact same rules as for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element


"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
import time
start_time = time.clock()
print start_time
print("--- %s seconds ---" % (time.clock() - start_time))

OSM_PATH = 'alamedamap.OSM'

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']
global expected  
global expected_state
global expected_country
global mapping


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    node_tag = {}
    way_node = {}
    way_tag = {}
    way_nodes = []
    id_list = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    expected_state = ["CA"]
    expected_country = ["US"]
    expected_name = ["name"]
    mapping = { "Ca": "CA",
            "ca": "CA",
            "California": "CA",
            "USA": "US",
            "UA": "US",
            "ON": "ON",
            "nam": "name",
            "name:prefix": "name",
            "name:zh": "name:zh",
            "name:vi": "name:vi",
            "name_1": "name_1",
            "name_2": "name_2",
            "name_3": "name_3"
             }        
 

    if element.tag == 'node':
        for attrib in NODE_FIELDS:
            if element.get(attrib):
                node_attribs[attrib] = element.attrib[attrib]
            else:
                node_attribs[attrib] = "99999999"

        for child in element:
            node_tag = {}
            if child.attrib['k'] == 'addr:state':
                expected = expected_state
                child.attrib['v'] = update_name(child.attrib['v'] , mapping, expected)
            if child.attrib['k'] == 'addr:country':
                expected = expected_country
                child.attrib['v'] = update_name(child.attrib['v'] , mapping, expected)
            if child.attrib['k'] == 'nam':
                expected = expected_name
                child.attrib['k'] = update_name(child.attrib['k'] , mapping, expected)
            if child.attrib['k'] == 'phone':
                child.attrib['v'] = fixphone(child.attrib['v'])    

            if PROBLEMCHARS.match(child.attrib["k"]):
                continue
            elif LOWER_COLON.match(child.attrib["k"]):
                node_tag["type"] = child.attrib["k"].split(":",2)[0]
                node_tag["key"] = child.attrib["k"].split(":",2)[1]
                node_tag["id"] = element.attrib["id"]
                node_tag["value"] = child.attrib["v"]
                tags.append(node_tag)
            else:
                node_tag["type"] = "regular"
                node_tag["key"] = child.attrib["k"]
                node_tag["id"] = element.attrib["id"]
                node_tag["value"] = child.attrib["v"]
                tags.append(node_tag)
                


        return {'node': node_attribs, 'node_tags': tags}
    
    elif element.tag == 'way':
        for attrib in element.attrib:
            if attrib in WAY_FIELDS:
                way_attribs[attrib] = element.attrib[attrib]
        for child in element:
            way_node = {}
            way_tag = {}
                                   
            if child.tag == "nd":
                if element.attrib["id"] not in id_list:
                    i=0
                    id_list.append(element.attrib["id"])
                    way_node["id"] = element.attrib["id"]
                    way_node["node_id"] = child.attrib["ref"]
                    way_node["position"] = i
                    way_nodes.append(way_node)
                else:
                    i=i+1
                    way_node["id"] = element.attrib["id"]
                    way_node["node_id"] = child.attrib["ref"]
                    way_node["position"] = i
                    way_nodes.append(way_node)
            if child.tag == "tag":
                if child.attrib['k'] == 'addr:state':
                    expected = expected_state
                    child.attrib['v'] = update_name(child.attrib['v'] , mapping, expected)
                if child.attrib['k'] == 'addr:country':
                    expected = expected_country
                    child.attrib['v'] = update_name(child.attrib['v'] , mapping, expected)
                if child.attrib['k'] == 'nam':
                    expected = expected_name
                    child.attrib['k'] = update_name(child.attrib['k'] , mapping, expected)                
                if child.attrib['k'] == 'phone':
                    child.attrib['v'] = fixphone(child.attrib['v']) 
                    
                if PROBLEMCHARS.match(child.attrib["k"]):
                    continue
                elif LOWER_COLON.match(child.attrib["k"]):
                    way_tag["type"] = child.attrib["k"].split(":",1)[0]
                    way_tag["key"] = child.attrib["k"].split(":",1)[1]
                    way_tag["id"] = element.attrib["id"]
                    way_tag["value"] = child.attrib["v"]
                    tags.append(way_tag)
            
                else:
                    way_tag["type"] = "regular"
                    way_tag["key"] = child.attrib["k"]
                    way_tag["id"] = element.attrib["id"]
                    way_tag["value"] = child.attrib["v"]
                    tags.append(way_tag)
#        pprint.pprint(tags)

        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
    

    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #

def update_name(name, mapping, expected):
    """Update the value of any tag or value in xml file based on a mapping table"""
    m = name
    if m not in expected:
        name=mapping[name]   
    return name 
    
def fixphone(numberin):
    """Standardize phone number entries in xml file"""
#Check if string is ASCII    

    numberout = numberin
    numberout =re.sub('[^0-9]','', numberin)
     
    if len(numberout) == 10:
       """valid number - format it"""
       numberout = "(" + numberout[0:3] + ") " + numberout[3:6] + "-" + numberout[6:]        
    elif len(numberout) == 11:
        if numberout[0] == '1':
            """valid number - strip the 1 and format it"""    
            numberout = "(" + numberout[1:4] + ") " + numberout[4:7] + "-" + numberout[7:]    
        else:
            """invalid number - leave it alone"""
            numberout = numberin                        
   
    print numberout
         
    return numberout 
    
    
    
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
    end_time = time.clock()
    print end_time
    print("--- %s seconds ---" % (time.clock() - start_time))
    print("--- %s seconds ---" % (end_time - start_time))    
    print "Process Map Complete"
    

            
   
