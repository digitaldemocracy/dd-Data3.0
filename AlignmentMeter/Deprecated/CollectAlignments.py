import docx
from io import StringIO
import re
import docx2txt
from datetime import datetime
import pickle
import glob

data_dir = 'BillAnalysisLobs/'

bill_types = ['AB',
'ACA',
'ACR',
'AJR',
'HR',
'SB',
'SCA',
'SCR',
'SJR',
'SR',
'ABX1',
'SBX1',
'SCAX1',
'SCRX1',
'SRX1',
'ABX2',
'ACRX2',
'SBX2',
'SCRX2',
'SRX2',
'BUD',
'NON']

bill_types_str = r'|'.join(bill_types)

def grab_info(text, doc_name):

    try:
        date_match = re.search('Date of Hearing:\s+ (\w+ \d{1,2}, \d{4})', text)
        date = date_match.group(1)

        date = datetime.strptime(date, "%B %d, %Y")
        date = date.date()
    except AttributeError:
        print('Failed on matching date: ' + doc_name)


    try:
        bill_re = "(%s)\s+\d{1,4}" % bill_types_str
        bill_match = re.search(bill_re, text)

        bill = bill_match.group(0)
    except AttributeError:
        print('Failed on matching bill: ' + doc_name)


    try:
        pos_match = re.search(r'SUPPORT / OPPOSITION:\s+Support\s+(.*)\s+Opposition\s+(.*)\s+Analysis Prepared by:', text,
                              re.DOTALL)

        support = re.split('\n', pos_match.group(1).strip())
        opposition = re.split('\n', pos_match.group(2).strip())

    except AttributeError:
        print('Failed on matching positions: ' + doc_name)

    out = {'bill': bill,
           'date': date,
           'support': support,
           'opposition': opposition}

    return out


def main():
    positions = []
    for doc_name in glob.glob(data_dir + '*.lob'):
        text = docx2txt.process(data_dir + doc_name)
        info = grab_info(text, doc_name)
        positions.append(info)

    pickle.dump(positions, open('scraped_positions.p', 'wb'))


if __name__ == '__main__':
    main()


