import re
import unidecode


"""checks to see if the organization is an actual org,
  and not some kind of filler. Returns a new list, cleaned
  of all problem organizations """
def cleanOrgs(orgs):
    reBill = r'[A-Z]{2,3}\s*\d+'
    rePage = r'Page\s*\d+'
    rePrivate = r'\w* ?[Pp]rivate [Ii]ndividuals'
    reNone = r'\s*[Nn]one.*?'
    reWhiteSpace = r"\S"
    reNoOpposition = r"[Nn]o [Oo]pposition"
    reVerified = r'\(Verified.+\)'
    reDateHearing = r'\Date of Hearing'
    reNumIndividuals = r'Numerous Individuals'
    reOneIndividual = r'one individual'
    reAuthor = r'Authors?'

    output = []

    for org in orgs:

        # You're trying to stop your opposition from 
        # overrunning here
        # You also do this is scrapeCase2 soo, yea
        if re.match(reNone, org.strip()):
            break
        if org != "" \
            and re.search(reWhiteSpace, org) \
            and not re.search(reBill, org) \
            and not re.search(rePage, org) \
            and not re.search(rePrivate, org) \
            and not re.match(reNone, org) \
            and not re.search(reNoOpposition, org) \
            and not re.search(reVerified, org) \
            and not re.search(reDateHearing, org) \
            and not re.search(reNumIndividuals, org) \
            and not re.search(reOneIndividual, org, re.IGNORECASE) \
            and not re.match(reAuthor, org.strip(), re.IGNORECASE):

            # catches the case of a / dividing two orgs
            split = org.split("/")
            org = split[0]
            orgs.extend(split[1:])

            output.append(org)

    return output

""" Spits up the string containing all the organizations.
  returns a list of all the orgs """
def splitOrgs(org_string):
    lines = re.split('\n', org_string)
    cleanedLines = cleanOrgs(lines)
    # Strips the last line of whitespace so your partial
    # line test will work
    lines[-1] = lines[-1].strip()
    orgs = []

    partial = False
    for line in cleanedLines:
        # append to last entry if the last entry was a partial 
        # org
        if partial:
            orgs[-1] = orgs[-1] + " " + line.strip()
        else:
            orgs.append(line.strip())
        # Confirms is this line is a partial org
        if line[-2:] == "  ":
            partial = True
        else:
            partial = False
    final = []
    # Removes any words in parenthesis from the org name.
    # e.g. (co-sponsor)
    for org in orgs:
        org = re.sub(r'\(.*?\)', '', org).strip() 
        if "*" not in org:
            final.append(org)

    return final

"""Writes the support and opposition to a csv file"""
def writeOutput(input_orgs, out_file):
        output_orgs = []
        output_orgs = splitOrgs(input_orgs)

        with open(out_file, 'w') as output:
            first = True
            for org in output_orgs:
                org = unidecode.unidecode(org).lower().title()
                if first:
                    first = False
                    output.write('"{}"'.format(org))
                else:

                    output.write(',\n"{}"'.format(org))


"""Scrapes one possible output of bill analysis. Writes support and opposition to csvs"""
def scrapeCase1(analysis, supportOut, opposeOut):
    regex = (r'(Sponsor:?\s+)(.*?)'
             r'(Support:?\s+)(.*?)'
             r'((Opposition|Oppose):?\s*)'
             r'(.*)(Analysis|-- END)')

    match = re.search(regex, analysis, re.DOTALL)

    sponsor = match.group(2)
    support = match.group(4)
    opposition = match.group(7)

    support = sponsor + '\n' + support

    writeOutput(support, supportOut)
    writeOutput(opposition, opposeOut)


"""Scrapes one possible output of bill analysis. Writes support and opposition to csvs"""
def scrapeCase2(analysis, supportOut, opposeOut):
    regex = r'(SUPPORT\s*/\s*OPPOSITION)(.*)'
    match = re.search(regex, analysis, re.DOTALL|re.IGNORECASE)

    section = match.group(2)
    regex = (r'([^\S\n]*Support\s*)(\n[^\S\n]*.*?)'
        r'(Opposition)(\s*)'
    r'(.*)\s+(Analysis|-- END)')

    # These lines were doing weird stuff
    # match = re.search(regex, section, re.DOTALL)
    # numNewLines = match.group(4).count('\n')
    # regex = regex + r'\n'*(numNewLines + 1)

    match = re.search(regex, section, re.DOTALL)

    support = match.group(2)
    opposition = match.group(5)

    writeOutput(support, supportOut)
    writeOutput(opposition, opposeOut)


"""Scrapes one possible output of bill analysis. Writes support and opposition to csvs"""
def scrapeCase3(analysis, supportOut, opposeOut):
    regex = r'(SUPPORT\s*AND\s*OPPOSITION)(.*)'
    match = re.search(regex, analysis, re.DOTALL|re.IGNORECASE)

    section = match.group(2)
    regex = (r'(Support:\s*)(.*?)'
             r'((Opposition|Oppose):\s*)'
             r'(.*)(Analysis|-- END)')
    # r'(.*?)((\n[^\S\n]*\n[^\S\n]*)')

    match = re.search(regex, section, re.DOTALL)

    support = match.group(2)
    opposition = match.group(5)

    writeOutput(support, supportOut)
    writeOutput(opposition, opposeOut)


"""Scrapes one possible output of bill analysis. Writes support and opposition to csvs"""
def scrapeCase4(analysis, supportOut, opposeOut):
    regex = (r"(\n\s*|</u>\s)(SUPPORT|Support)[:\n\s](.*?)\n+"
             r"(OPPOSITION|Oppose):?\s+(.*?)\n+(-- end|analysis)")

    match = re.search(regex, analysis, re.DOTALL | re.M | re.IGNORECASE)

    # print match.group()

    support = match.group(3)
    opposition = match.group(5)

    writeOutput(support, supportOut)
    writeOutput(opposition, opposeOut)


"""Scrapes one possible output of bill analysis. Writes support and opposition to csvs"""
def scrapeCase5(analysis, supportOut, opposeOut):
    regex = (r"\n\t*(Support)[:\n\s](.*?)"
             r"(OPPOSITION|Oppose):?\s+(.*?)(\n{3,}|\Z)")

    match = re.search(regex, analysis, re.DOTALL | re.M | re.IGNORECASE)

    support = match.group(2)
    opposition = match.group(4)

    writeOutput(support, supportOut)
    writeOutput(opposition, opposeOut)


"""Actually does the scraping of a bill analysis. Returns tuple indicating which case was scraped"""
def BillScrape(text, supportOut, opposeOut):
    # Add or remove these based on whether you're running on Windows or Unix
    text = text.replace('\r\n', '\n')
    text = text.replace("\\'", "'")

    if r'\x0c' in text:
        # text = re.sub(r'\x0c', r'\n', text)
        text = text.replace(r'\x0c', '')

    num_empty = 0
    case_1 = 0
    case_2 = 0
    case_3 = 0
    case_4 = 0
    case_5 = 0

    try:
        scrapeCase1(text, supportOut, opposeOut)
        # print("Case 1")
        case_1 += 1
    # This means that the regex couldn't match anything
    except AttributeError:
        try:
            scrapeCase2(text, supportOut, opposeOut)
            # print("Case 3")
            case_2 += 1

        except AttributeError:
            try:
                scrapeCase3(text, supportOut, opposeOut)
                # print("Case 2")
                case_3 += 1
            except AttributeError:
                try:
                    scrapeCase4(text, supportOut, opposeOut)
                    # print("Case 4")
                    case_4 += 1
                except AttributeError:
                    try:
                        scrapeCase5(text, supportOut, opposeOut)
                        # print("Case 5)
                        case_5 += 1
                    except AttributeError:
                        # print("No test cases")
                        num_empty += 1

    return case_1, case_2, case_3, case_4, case_5, num_empty







    




    
