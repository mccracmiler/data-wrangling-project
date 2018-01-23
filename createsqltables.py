#reference: http://www.sqlitetutorial.net/sqlite-python/create-tables/

"""First, create a Connection object using the connect() function of the sqlite3 module.
    Second, create a Cursor object by calling the cursor() method of the Connection object.
    Third, pass the CREATE TABLE statement to the execute() method of the Cursor object, 
    and execute this method."""
 
###First, develop a function called create_connection() that 
###returns a Connection object, which represents an SQLite database 
###specified by the database file parameter db_file.

import csv
import sqlite3
import pandas as pd


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None
    
###Second, develop a function named create_table() that accepts a 
###Connection object and an SQL statement. Inside the function, we call 
###the execute() method of the Cursor object to execute the CREATE TABLE statement.
    
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        print "Success creating table: "
    except:
        print "Error creating table: "
                  
def main():
    database = "openstreetmaps.db"
 
    sql_create_nodes_table = """CREATE TABLE nodes (
                                        id INTEGER PRIMARY KEY NOT NULL,
                                        lat REAL,
                                        lon REAL,
                                        user TEXT,
                                        uid INTEGER,
                                        version INTEGER,
                                        changeset INTEGER,
                                        timestamp TEXT
                                    );"""

    sql_create_nodes_tags_table = """CREATE TABLE nodes_tags (
                                        id INTEGER,
                                        key TEXT,
                                        value TEXT,
                                        type TEXT,
                                        FOREIGN KEY (id) REFERENCES nodes(id)
                                    );"""

    sql_create_ways_table = """CREATE TABLE ways (
                                        id INTEGER PRIMARY KEY NOT NULL,
                                        user TEXT,
                                        uid INTEGER,
                                        version TEXT,
                                        changeset INTEGER,
                                        timestamp TEXT
                                    );"""

    sql_create_ways_tags_table = """CREATE TABLE ways_tags (
                                        id INTEGER NOT NULL,
                                        key TEXT NOT NULL,
                                        value TEXT NOT NULL,
                                        type TEXT,
                                        FOREIGN KEY (id) REFERENCES ways(id)
                                    );"""
                                    
    sql_create_ways_nodes_table = """CREATE TABLE ways_nodes (
                                        id INTEGER NOT NULL,
                                        node_id INTEGER NOT NULL,
                                        position INTEGER NOT NULL,
                                        FOREIGN KEY (id) REFERENCES ways(id),
                                        FOREIGN KEY (node_id) REFERENCES nodes(id)
                                    );"""

    # create a database connection
    conn = create_connection(database)
    if conn is not None:
    # create tables
        create_table(conn, sql_create_nodes_table)
        create_table(conn, sql_create_nodes_tags_table)
        create_table(conn, sql_create_ways_table)
        create_table(conn, sql_create_ways_nodes_table)
        create_table(conn, sql_create_ways_tags_table)
    else:
        print("Error! cannot create the database connection.")
        
#main() function.
        
if __name__ == '__main__':
    main()
    conn = sqlite3.connect("openstreetmaps")
    cursor = conn.cursor()
    conn.close()