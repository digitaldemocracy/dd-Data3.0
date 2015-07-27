'''
File: Name_Fixes_Legislator_Migrate.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used when Legislator names change in capublic to what we have in DDDB2015Apr
- Used in legislator_migrate.py to adjust the names if they are the same Person

'''

import re
import sys

# If the name changes, place the name here and change it to the correct one
def clean_name_legislator_migrate(last, first):
	if(first == "Franklin" and last == "Bigelow"):
		first = "Frank"
	if(first == "Patricia " and last == "Bates"):
		first = "Pat"
	if(first == "Steven" and last == "Glazer"):
		first = "Steve"
	name = first + "<SPLIT>" + last
	return name
