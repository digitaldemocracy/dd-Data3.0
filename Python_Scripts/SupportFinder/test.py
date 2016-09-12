import re

def scrapeCase2(analysis, supportOut, opposeOut):
    regex = (r"(\n\s*|</u>\s)(SUPPORT|Support)[:\n\s](.*?)"
    r"(OPPOSITION|Oppose)[:\n](.*?\n)\s*(\S.*?)\n\n")

    # regex = (r"\n\s*(SUPPORT|Support)[:\n](.*?)")
    # regex = (r"\n\s*SUPPORT")

    match = re.search(regex, analysis, re.DOTALL|re.M)

    # print match.group()


    support = match.group(3)
    firstOpp = match.group(5)
    # print firstOpp
    # checks to see if your first match says None
    # we want to stop if this is the case
    if re.match(r'None', firstOpp.strip(), re.I):
        opposition = '';
    else:
        opposition = firstOpp + match.group(6)


    return support, opposition



kSupportFile = "support.csv"
kOpposeFile = "opposition.csv"
kDirPath = 'TestCases/'

text = open(kDirPath +  "AB_101_06-04-2015.txt", 'r').read()

support, oppostion = scrapeCase2(text, kSupportFile,
        kOpposeFile)

# reg = r'(Hello|World:)'
# match = re.search(reg, 'World', re.I)
# print match.group()

