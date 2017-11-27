import pymysql
import sys
import sqlparse
import re

"""
File: sql_wrapper.py
Author: Andrew Voorhees
Date: 11/4/2017

Description:
    - Simple script that wraps SQL files and executes each statement (there must be a semi-colon) separately. If there
      is an exception, it ensures that all table drop statements are still run. 
      
      *Note* All commented lines are filtered out so this depends on mysql comment syntax
    
Usage: 
    python sql_wrapper.py <path to file>
"""

CONN_INFO = {
             'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'dbMaster',
             'passwd': 'BalmerPeak'
             }


def get_tables(cursor):
    """Returns existing tables, views as sets"""

    q = """SHOW FULL TABLES IN {db}""".format(**CONN_INFO)
    cursor.execute(q)

    tables = set()
    views = set()
    for name, kind in cursor:
        if kind == 'VIEW':
            views.add(name)
        elif kind == 'BASE TABLE':
            tables.add(name)
        else:
            assert False
    return tables, views


def run_drop_stmts(cursor, to_drop):
    """Runs all the drop statements. Meant to be used if SQL script throws error"""
    tables, views = get_tables(cursor)

    drop_stmt = """DROP {} {}"""
    for t in to_drop:
        if t in views:
            cursor.execute(drop_stmt.format('VIEW', t))
        elif t in tables:
            cursor.execute(drop_stmt.format('TABLE', t))


def parse_sql(sql):
    """Returns individual sql statements and all tables that should be dropped if there is
       an error"""
    parsed = sqlparse.split(sql)

    stmts = []
    to_drop = []
    for stmt in parsed:
        stmt = sqlparse.format(stmt, strip_comments=True).strip()

        match = re.match(r'(drop)(.+)\s(\w+);', stmt, re.I | re.S)
        if match:
            to_drop.append(match.group(3))

        if stmt:
            stmts.append(stmt)

    return stmts, to_drop


def main():
    file_path = sys.argv[1]

    file = open(file_path, 'r')
    sql = file.read()
    file.close()

    stmts, to_drop = parse_sql(sql)

    cnxn = pymysql.connect(**CONN_INFO)
    cursor = cnxn.cursor()

    try:
        for stmt in stmts:
            cursor.execute(stmt)
    except pymysql.InternalError as e:
        run_drop_stmts(cursor, to_drop)
    finally:
        cursor.close()

    cnxn.close()


if __name__ == '__main__': 
    main()