"""
CA IMPORT BEHESTS 2023 (Updated script)

Author: Sarah Ellwein
Modified by:
Date: 2/22/2023
Last Updated:

Source: 
* California Fair Political Practices Commission spreadsheets
    https://www.fppc.ca.gov/content/dam/fppc/NS-Documents/BehestedExcel/BehestedPayments.xls
    
TO DO LIST:

* Make SQL insertion statements for missing data
* Further normalize names (correct misspelled names, remove middle name, etc.)
* Double check for corrections

"""

import numpy as np
import pandas as pd
import pymysql.cursors
from datetime import datetime

behest_link = "https://www.fppc.ca.gov/content/dam/fppc/NS-Documents/BehestedExcel/BehestedPayments.xls"
behests = pd.read_excel(behest_link, parse_dates=['DateOFPayment'])
behests['DateOFPayment'] = behests['DateOFPayment'].dt.date

# Database configurations
DB_CONFIG = {
    'host': '',
    'user': '',
    'pass': '',
    'db': '',
}

NAME_EXCEPTIONS = {
    "Achadijan, Katcho": "Achadjian, K.H. \"Katcho\"",
    "Achadjian, Katcho": "Achadjian, K.H. \"Katcho\"",
    "Allen, Ben": "Allen, Benjamin",
    "Bates, Pat": "Bates, Patricia",
    "Bonilla, Susan A": "Bonilla, Susan",
    "Bonilla, Susan A.": "Bonilla, Susan",
    "Brown, Jr., Edumund G": "Brown, Edmund",
    "Brown, Jr., Edmund G": "Brown, Edmund",
    "Brown Jr., Edmund G.": "Brown, Edmund",
    "Brown, Edmund Jr.": "Brown, Edmund",
    "Brown Jr., Edmund": "Brown, Edmund",
    "Calderon, Charles": "Calderon, Ian Charles",
    "Calderon, Ian": "Calderon, Ian Charles",
    "Cannella Anthony": "Cannella, Anthony",
    "Cedilo, Gilbert": "Cedillo, Gil",
    "Chu, Kasen": "Chu, Kansen",
    "Correa, Luis": "Correa, Lou",
    "Chau, Edwin": "Chau, Ed",
    "Dahle Brian": "Dahle, Brian",
    "DeLeon, Kevin": "De Leon, Kevin",
    "DeSauiner, Mark": "DeSaulnier, Mark",
    "Dickinson, Rogert": "Dickinson, Roger",
    "Dodd, William": "Dodd, Bill",
    "Eggman, Susan": "Eggman, Susan Talamantes",
    "Emmerson, William": "Emmerson, Bill",
    "Frazier, James": "Frazier, Jim",
    'Gaines, Edward ""Ted""': "Gaines, Ted",
    "Gaines, Edward (Ted)": "Gaines, Ted",
    "Garcia, Christina": "Garcia, Cristina",
    "Glazer, Steven": "Glazer, Steve",
    "Hall, Isadore III": "Hall, Isadore",
    "Harman, Thomas": "Harman, Tom",
    "Hernandez, Edward": "Hernandez, Ed",
    "Holden, Christopher": "Holden, Chris",
    "Jackson, Hanna- Beth": "Jackson, Hannah-Beth",
    "Jim Frazier": "Frazier, Jim",
    "Jones-Sawyer, Reginald Byron": "Jones-Sawyer, Reginald",
    "Jones-Sawyer, Sr., Reginald Byron": "Jones-Sawyer, Reginald",
    "Knight, Steve": "Knight, Stephen",
    "Lackey, Thomas": "Lackey, Tom",
    "LaMalfa, Doug": "La Malfa, Doug",
    "Lockyer, William": "Lockyer, Bill",
    "McLeod -Negrete, Gloria": "Negrete McLeod, Gloria",
    "Nielsen, James": "Nielsen, Jim",
    "Pan Richard": "Pan, Richard",
    "Perea, Henry T.": "Perea, Henry",
    "Rodriquez, Freddie": "Rodriguez, Freddie",
    "Salas Jr., Rudy": "Salas, Rudy",
    "Sales Jr. Rudy": "Salas, Rudy",
    "Salas Jr., Ruby": "Salas, Rudy",
    "Simitian, Joseph": "Simitian, Joe",
    "Steinberg, Darryl": "Steinberg, Darrell",
    "Stone, Jeffrey": "Stone, Jeff",
    "Swanson, Sandre'": "Swanson, Sandre",
    "Thomas-Ridley, Sebastian": "Ridley-Thomas, Sebastian",
    "Ting, Phil": "Ting, Philip",
    "Vidak, James Andy": "Vidak, Andy",
    "Wieckowski, Robert": "Wieckowski, Bob",
    "Wiener,Scott": "Wiener, Scott"
}


def match_name(name):
    if name in NAME_EXCEPTIONS.keys():
        return NAME_EXCEPTIONS[name]
    return name


behests['Official'] = behests['Official'].str.strip()
behests['Official'] = behests['Official'].apply(match_name)
behests[['last', 'first']] = behests['Official'].str.split(', ', expand=True)
behests = behests.drop(columns='Official')

# Connect to the database
connection = pymysql.connect(host=DB_CONFIG['host'],
                             user=DB_CONFIG['user'],
                             password=DB_CONFIG['pass'],
                             database=DB_CONFIG['db'],
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
        sql = "SELECT pid, `last`, `first` FROM Person;"
        cursor.execute(sql)
        pids = pd.DataFrame(cursor.fetchall())

        sql = "SELECT pid, `year` FROM Term WHERE `state` = 'CA';"
        cursor.execute(sql)
        terms = pd.DataFrame(cursor.fetchall())

        sql = "SELECT max(datePaid) AS lastDate FROM Behests;"
        cursor.execute(sql)
        lastDate = cursor.fetchall()[0]['lastDate']

pids = terms.merge(pids, how='left', on='pid')


def get_session_year(year):
    if year % 2 == 0:
        return year - 1
    return year


behests['year'] = behests['PaymentYear'].apply(get_session_year)

# Normalize first and last names with upper() function then merge
behests['last'] = behests['last'].str.upper()
behests['first'] = behests['first'].str.upper()
pids['last'] = pids['last'].str.upper()
pids['first'] = pids['first'].str.upper()

df = behests.merge(pids, how='left', on=['last', 'first', 'year'])

missing = df[df['pid'].isna()]

# Records that may be backfilled into DD DB
new_records = missing[missing['DateOFPayment'] > lastDate]

# Missing information (missing pid or term)
# NOTE: some names may be crossed out due to normalization issues
# missing[missing['DateOFPayment'] <= lastDate][['last', 'first']].drop_duplicates()

# SQL insertion. Wait to do it because database
