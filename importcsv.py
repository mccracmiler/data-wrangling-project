#This code is the second half of the Data Wrangling project.
#The scripts import maps data that has been scrapedand 

import csv
import sqlite3
import pandas as pd
import psycopg2 
from sqlalchemy import create_engine

NODES = pd.read_csv("nodes.csv")
NODE_TAGS = pd.read_csv("nodes_tags.csv")
WAYS = pd.read_csv("ways.csv")
WAY_NODES = pd.read_csv("ways_nodes.csv")
WAY_TAGS = pd.read_csv("ways_tags.csv")


#con = sa.create_engine('postgresql:////localhost/openstreetmaps.db')
#NODES = pd.read_csv('nodes.csv', chunksize=100000)
#for node in NODES:
#    node.to_sql(name='nodes', if_exists='append', con=con)


eng = create_engine('openstreetmaps.db')
conn = eng.connect()
NODES.to_sql('nodes', conn, if_exists='append')    
#NODE_TAGS.to_sql('node_tags',conn)      
#WAYS.to_sql('ways',conn)
#WAYS_NODES.to_sql('ways_nodes',conn)         
#WAYS_TAGS.to_sql('ways_tags',conn) 

#http://odo.pydata.org/en/latest/perf.html


#sql
#SELECT tags.value, COUNT(*) as count 
#FROM (SELECT * FROM nodes_tags 
#	  UNION ALL 
#      SELECT * FROM ways_tags) tags
#WHERE tags.key='postcode'
#GROUP BY tags.value
#ORDER BY count DESC;



