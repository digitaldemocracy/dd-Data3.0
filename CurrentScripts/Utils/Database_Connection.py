#!/usr/bin/env python

"""
File: Database_Connection.py
Author: Nick Russo
Date: 5th July 2017

Description:
- Connection Function for connecting to a database for running/testing scripts.
- Meant to help prevent accidental insertions into the live database.
"""
from __future__ import print_function
import sys
import MySQLdb
import time
import socket

def countdown(db):
    '''
    Prints a countdown. Used as a buffer just in case a developer wants to cancel a script from running.
    :return: Nothing
    '''
    for x in range(5, -1, -1):
        time.sleep(1)
        sys.stdout.write('\r' + "Running on " + db + " database in " + str(x) + " Seconds")
        sys.stdout.flush()  # important
    # Creates separation from other print statements
    print()


def connect(db = None):
    '''
    Returns a MySQLdb connection to the specified database.
    Checks the current IP address and checks against the dw server. If the script is running on the
    dw server then the check is skipped.
    :param db: string: live for live database, local for local host database, defaults to dev server
    :param override_flag: override_flag forces the scripted to run on the live server
    :return: a MySQLdb connection to the specified database
    '''
    # if socket.gethostbyname(socket.gethostname()) == "172.31.37.21" or db == "force" or \
    #       (db == "live" and raw_input("Are you sure you want this script on the live database? (y/n) ").lower() == "y"):
    #     countdown("live")
    #     return MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
    #                      port=3306,
    #                      db='DDDB2016Aug',
    #                      user='awsDB',
    #                      passwd='digitaldemocracy789',
    #                      charset='utf8')
    # else:
    #elif db == "local":
        #countdown("local")
    return MySQLdb.connect(host='localhost',
                    port=3306,
                    db='DDDB2016Aug',
                    user='root',
                    passwd='',
                    charset='utf8')
    # else:
    #     print("Running on Dev DB")
    #     return MySQLdb.connect(host='dev.digitaldemocracy.org',
    #                      port=3306,
    #                      db='parose_dddb',
    #                      user='awsDB',
    #                      passwd='digitaldemocracy789',
    #                      charset='utf8')

def connect_to_capublic():
    '''
    Returns a MySQLdb connection to capublic on the transcription server.
    :return: a MySQLdb connection
    '''
    return MySQLdb.connect(host='transcription.digitaldemocracy.org',
                             user='monty',
                             db='capublic',
                             passwd='python',
                             charset='utf8')



