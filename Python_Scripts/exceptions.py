import re
import sys

def clean_name_legislator_migrate(last, first):
	if(first == "Franklin" and last == "Bigelow"):
		first = "Frank"
	if(first == "Patricia" and last == "Bates"):
		first = "Pat"
	name = first + "<SPLIT>" + last
	return name
	

if __name__ == "__main__":
	clean_name_legislator_migrate("blah", "blah")