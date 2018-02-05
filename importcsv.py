#This code is the second half of the Data Wrangling project.
#The scripts import maps data that has been scrapedand 

import csv
import sqlite3
import pandas as pd
import psycopg2 
import pprint
from sqlalchemy import create_engine

NODES = pd.read_csv("nodes.csv",encoding='utf-8')
print "NODES read to pd"
NODE_TAGS = pd.read_csv("nodes_tags.csv",encoding='utf-8')
print "NODES  Tags read to pd"
WAYS = pd.read_csv("ways.csv",encoding='utf-8')
print "ways read to pd"
WAYS_NODES = pd.read_csv("ways_nodes.csv",encoding='utf-8')
print "WAYS NODES read to pd"
WAYS_TAGS = pd.read_csv("ways_tags.csv",encoding='utf-8')
print "WAYS TAGS read to pd"

eng = create_engine("sqlite://", encoding='utf8')
conn = eng.connect()
NODES.to_sql('nodes', conn)
print "NODES ADDED to TBL"    
NODE_TAGS.to_sql('node_tags',conn) 
print "NODE_TAGS ADDED to TBL"        
WAYS.to_sql('ways',conn)
print "WAYS_NODES ADDED to TBL" 
WAYS_NODES.to_sql('ways_nodes',conn) 
print "WAYS_NODES ADDED to TBL"         
WAYS_TAGS.to_sql('ways_tags',conn) 
print "WAYS_TAGS ADDED to TBL" 
print "Process complete"


###Record Count Metric database queries
numberofnodes = pd.read_sql("SELECT COUNT(*) FROM nodes",eng)
numberofnode_tags = pd.read_sql("SELECT COUNT(*) FROM node_tags",eng)
numberofways = pd.read_sql("SELECT COUNT(*) FROM ways",eng)
numberofways_tags = pd.read_sql("SELECT COUNT(*) FROM ways_tags",eng)
numberofways_nodes = pd.read_sql("SELECT COUNT(*) FROM ways_nodes",eng)
###Reporting file Statistics 
raw_data = {'Table_name': ['Nodes', 'Node Tags', 'Ways', 'Ways_Tags', 'Ways_Nodes'],}
df_a = pd.DataFrame(raw_data, columns = ['Table_name'])
dfall = [numberofnodes,numberofnode_tags,numberofways,numberofways_tags,numberofways_nodes]
result = pd.concat(dfall)
result.columns = ['Records']
result = result.set_index([[0, 1, 2, 3, 4, ]])
result2 = pd.concat([df_a,result],axis=1)
print result2


#Timestamps
most_recentnode = pd.read_sql("SELECT DISTINCT timestamp \
        FROM nodes Order by timestamp DESC Limit 2",eng)
print "Most Recent node updates:", most_recentnode  

oldestnode = pd.read_sql("SELECT DISTINCT timestamp \
        FROM nodes Order by timestamp Limit 2",eng)
print "Oldest node updates:", oldestnode  

most_recentways = pd.read_sql("SELECT DISTINCT timestamp \
        FROM ways Order by timestamp DESC Limit 2",eng)
print "Most Recent ways updates:", most_recentways  

oldestways = pd.read_sql("SELECT DISTINCT timestamp \
        FROM ways Order by timestamp Limit 2",eng)
print "Oldest ways updates:", oldestways  

##Top Amenities                                       
top_amenities = pd.read_sql("SELECT e.value, COUNT(*) as num \
        FROM (SELECT value from node_tags WHERE key = 'amenity' UNION ALL \
              SELECT value from ways_tags WHERE key = 'amenity') e \
              GROUP BY e.value ORDER BY num \
              DESC LIMIT 10",eng);
print "Top Amenities:", top_amenities

#Distinct Users
distinct_users = pd.read_sql("SELECT COUNT(DISTINCT du.uid) \
        FROM (SELECT DISTINCT uid FROM nodes) as du \
        JOIN (SELECT DISTINCT uid FROM ways) as wu \
        ON du.uid = wu.uid",eng);
print "Distinct Users:", distinct_users 

#Most active users
most_active_users = pd.read_sql("SELECT e.user, COUNT(*) as num \
        FROM (SELECT user from NODES UNION ALL \
              SELECT user from ways) e \
              GROUP BY e.user ORDER BY num DESC LIMIT 5",eng);
print "Most active contributors:", most_active_users


notredame = pd.read_sql("SELECT key, value FROM node_tags \
        WHERE id IN (SELECT id FROM node_tags WHERE value = \
        'Udacity')",eng);
notredame = notredame.head(12)

stcharles = pd.read_sql("SELECT key, value FROM ways_tags \
        WHERE id IN (SELECT id FROM ways_tags WHERE value = \
        'Stanford University' and key = 'name')",eng);
stcharles = stcharles.head(12)    

firstbaptist = pd.read_sql("SELECT key, value FROM ways_tags \
        WHERE id IN (SELECT id FROM ways_tags WHERE value = \
        'Carnegie Mellon University Silicon Valley' and key = 'name')",eng);
firstbaptist = firstbaptist.head(12) 
                   
compareall = pd.concat([notredame,stcharles,firstbaptist], axis=1)
compareall.to_csv("compareall.csv",encoding='utf-8')   
print compareall
                     