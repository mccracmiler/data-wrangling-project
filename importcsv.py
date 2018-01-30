#This code is the second half of the Data Wrangling project.
#The scripts import maps data that has been scrapedand 

import csv
import sqlite3
import pandas as pd
import psycopg2 
from sqlalchemy import create_engine

NODES = pd.read_csv("nodes.csv",encoding='utf-8')
NODE_TAGS = pd.read_csv("nodes_tags.csv",encoding='utf-8')
WAYS = pd.read_csv("ways.csv",encoding='utf-8')
WAYS_NODES = pd.read_csv("ways_nodes.csv",encoding='utf-8')
WAYS_TAGS = pd.read_csv("ways_tags.csv",encoding='utf-8')


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

#sql queries
distinct_users = pd.read_sql("SELECT COUNT(DISTINCT NODES.uid) as distinctusers \
        FROM NODES JOIN WAYS on WAYS.uid = NODES.uid",eng)
print "Distinct Users:", distinct_users  


amenities = pd.read_sql("SELECT value, COUNT(*) as num FROM NODE_TAGS \
        WHERE key='amenity' GROUP BY value ORDER BY num DESC \
        LIMIT 10",eng);
print amenities

distinct_users = pd.read_sql("SELECT COUNT(DISTINCT nodes.uid) as distinctusers \
        FROM nodes JOIN ways on ways.uid = nodes.uid",eng)
print "Distinct Users:", distinct_users  

#Most active users
most_active_nodes_users = pd.read_sql("SELECT user, COUNT(*) as num FROM nodes \
        GROUP BY user ORDER by num DESC Limit 3",eng);
print "Most active nodes contributors:", most_active_nodes_users

most_active_way_users = pd.read_sql("SELECT user, COUNT(*) as num FROM ways \
        GROUP BY user ORDER by num DESC Limit 3",eng);
print "Most active ways contributors:", most_active_way_users

getid = pd.read_sql("SELECT id FROM node_tags \
        WHERE value = 'Notre Dame de Namur University'",eng);
print "Get id:", getid

getdata = pd.read_sql("SELECT key, value FROM node_tags \
        WHERE id = '358763902'",eng);
print "Data:", getdata

getdata2 = pd.read_sql("SELECT key, value FROM node_tags\
        (SELECT id AS getid FROM node_tags WHERE value = 'Notre Dame de Namur University')\
          AS subquery WHERE id = getid",eng);
print "Data:", getdata

 