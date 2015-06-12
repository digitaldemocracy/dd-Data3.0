'''
File: Lobbying_Firm_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used to correct names of Lobbying Firms gathered during the Lobbying Info
- Used as an import to the Cal-Access-Accessor.py to clean Lobbying Firm Names
'''

import re
import sys
import string
import mysql.connector

def clean_name(name):
	flag = 0;
	if findRoman(name.split(' ')[len(name.split(' ')) - 1]) != 0:
		flag = 1;
	name = name.split(' ')
	count = lambda l1,l2: sum([1 for x in l1 if x in l2])
	for x in range(0, len(name) - flag):
		if (sum(1 for c in name[x] if c.isupper()) == len(name[x]) - count(name[x], string.punctuation)) or (sum(1 for c in name[x] if c.isupper()) == 0):
			if name[x] != "LLC" and name[x] != "LLP" and name[x] != "LP":
				name[x] = name[x].title()
	name = ' '.join(name)
	return name

romanNumeralMap = (('M',  1000),
                   ('CM', 900),
                   ('D',  500),
                   ('CD', 400),
                   ('C',  100),
                   ('XC', 90),
                   ('L',  50),
                   ('XL', 40),
                   ('X',  10),
                   ('IX', 9),
                   ('V',  5),
                   ('IV', 4),
                   ('I',  1))

#Define pattern to detect valid Roman numerals
romanNumeralPattern = re.compile("""
    ^                   # beginning of string
    M{0,4}              # thousands - 0 to 4 M's
    (CM|CD|D?C{0,3})    # hundreds - 900 (CM), 400 (CD), 0-300 (0 to 3 C's),
                        #            or 500-800 (D, followed by 0 to 3 C's)
    (XC|XL|L?X{0,3})    # tens - 90 (XC), 40 (XL), 0-30 (0 to 3 X's),
                        #        or 50-80 (L, followed by 0 to 3 X's)
    (IX|IV|V?I{0,3})    # ones - 9 (IX), 4 (IV), 0-3 (0 to 3 I's),
                        #        or 5-8 (V, followed by 0 to 3 I's)
    $                   # end of string
    """ ,re.VERBOSE)

def findRoman(s):
    """convert Roman numeral to integer"""
    flag = 0
    if not s:
        print "invalid"
    if romanNumeralPattern.search(s):
	print "found a roman numeral"
        flag = 1
	pass

    result = 0
    index = 0
    if(flag != 0):
    	for numeral, integer in romanNumeralMap:
        	while s[index:index+len(numeral)] == numeral:
            		result += integer
            		index += len(numeral)
    return result

def cleanNamesLobFirm():
	dd.execute("SELECT * from LobbyingFirm;")
	for x in range(0, dd.rowcount):
		try:
			temp = dd.fetchone()
			name = clean_name(temp[0])
			filer_id = temp[1]
			if(temp[0] != name):
				print temp[0]
				print name
				print x
				ddput.execute("UPDATE LobbyingFirm SET filer_naml = %s WHERE filer_id = %s;", (name, filer_id))
				print "Row(s) were updated :" +  str(ddput.rowcount)
		except:
			print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	db.commit()

def cleanNamesLobEmp():
	dd.execute("SELECT * from LobbyistEmployer;")
	for x in range(0, dd.rowcount):
		try:
			temp = dd.fetchone()
			name = clean_name(temp[0])
			le_id = temp[2]
			if(temp[0] != name):
				print temp[0]
				print name
				print x
				ddput.execute("UPDATE LobbyistEmployer SET filer_naml = %s WHERE le_id = %s;", (name, le_id))
				print "Row(s) were updated :" +  str(ddput.rowcount)
		except:
			print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	db.commit()

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '', buffered=True)
dd = db.cursor(buffered = True)
ddput = db.cursor(buffered = True)

if __name__ == "__main__":
	cleanNamesLobFirm()
	cleanNamesLobEmp()

db.close()
	
	
	

