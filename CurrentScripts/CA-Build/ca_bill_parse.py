#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
File: billparse.py
Author: ???
Modified By: Daniel Mangin, Mandy Chan, Steven Thon, Freddy Hernandez
Date: 6/11/2015

Description:
- Takes the bill_xml column from the capublic.bill_version_tbl
  and inserts it into the appropriate columns in DDDB.BillVersion
- This script runs under the update script

Sources:
- Leginfo (capublic)
  - Pubinfo_2015.zip
  - Pubinfo_Mon.zip
  - Pubinfo_Tue.zip
  - Pubinfo_Wed.zip
  - Pubinfo_Thu.zip
  - Pubinfo_Fri.zip
  - Pubinfo_Sat.zip

- capublic
  - bill_version_tbl

Populates:
  - BillVersion (title, digest, text, state)
'''
import json
import unicodedata
import datetime as dt
from lxml import etree
from Utils.Generic_Utils import *
from Utils.Database_Connection import *

logger = None
UPDATE = 0

# U.S. State
STATE = 'CA'

# Queries
QS_CPUB_BILL_VERSION = '''SELECT bill_version_id, bill_xml
                          FROM bill_version_tbl
                          WHERE trans_update > %(updated_since)s
                          '''
QU_BILL_VERSION = '''UPDATE BillVersion
                     SET title = %s, digest= %s, text = %s, state = %s
                     WHERE vid = %s'''


'''Tries to remove unmatched html tags from an xml string.
'''
def remove_unmatched_tags(xml):
    # The following regex courtesy of:
    # http://haacked.com/archive/2004/10/25/usingregularexpressionstomatchhtml.aspx/
    pat_str = r'''
    <(/?\w+)                                               # tag name
    ((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[\^'">\s]+))?)+\s*|\s*) # tag attributes
    /?>                                                    # self-closing'''
    tag_pat = re.compile(pat_str, re.DOTALL | re.VERBOSE)
    stack = []
    chars_deleted = 0

    def del_tag(xml, start, end):
        return xml[:start - chars_deleted] + xml[end - chars_deleted:]

    for m in re.finditer(tag_pat, xml):
        tag = m.group()
        tag_name = m.group(1)
        if tag_name.startswith('/'):
            if len(stack) == 0 or stack[-1].group(1) != tag_name[1:]:
                # Unmatched end tag. Get rid of it.
                xml = del_tag(xml, m.start(), m.end())
                chars_deleted += len(tag)
            else:
                stack.pop()
        else:
            # Don't add self-closing tags to the stack.
            if not tag.endswith('/>'):
                stack.append(m)

    # If the stack is not empty, then there are unmatched start tags.
    for m in stack:
        xml = del_tag(xml, m.start(), m.end())
        chars_deleted += len(m.group())
    return xml

'''The xml files from capublic have several issues. This function employs
several methods to attempt to fix them.
'''
def sanitize_xml(xml):
    # Bad strings present in the xml, along with their corresponding
    # replacements.
    replacement_patterns = [
        (r'&lt;xhtml:', '<'),   # Specific case 1.
        (r'&lt;/xhtml:', '</'), # Specific case 2.
        (r'&quot;', '"'),
        (r'<<', '<')
    ]

    xml = xml.strip()
    xml = re.sub(r'<\?xm-(insertion|deletion)_mark\?>', r'', xml)
    # Need to compile the pattern with flags because of Python 2.6.
    data_pat = re.compile(r'<\?xm-(insertion|deletion)_mark data="(.*?)"\?>',
                          re.DOTALL)
    xml = re.sub(data_pat, r'<span class="\1">\2</span>', xml)

    xml = re.sub(r'<\?xm-(insertion|deletion)_mark_start\?>',
                 r'<span class="\1">', xml)
    xml = re.sub(r'<\?xm-(insertion|deletion)_mark_end\?>', r'</span>', xml)
    for pat, repl in replacement_patterns:
        xml = re.sub(pat, repl, xml)
    xml = remove_unmatched_tags(xml)
    return xml

def get_bill_versions(ca_cursor):
    updated_date = dt.date.today() - dt.timedelta(weeks=1)
    updated_date = updated_date.strftime('%Y-%m-%d')

    ca_cursor.execute(QS_CPUB_BILL_VERSION, {'updated_since': updated_date})

    for vid, xml in ca_cursor.fetchall():
        if xml is None:
            continue
        yield '%s_%s' % (STATE, vid), sanitize_xml(xml)

def billparse(ca_cursor, dd_cursor):
    global UPDATE

    for vid, xml in get_bill_versions(ca_cursor):
        # This line will fail if |xml| is not valid XML.
        try:
            xml = unicodedata.normalize('NFKD', xml).encode('ascii', 'ignore')
            root = etree.fromstring(xml)
        except:
            raise

        def extract_caml(tag):
            pat = '<{0}:{1}.*?>(.*?)</{0}:{1}>'.format('caml', tag)
            return ''.join(node for node in re.findall(pat, xml, re.DOTALL))

        title = extract_caml('Title')
        digest = extract_caml('DigestText')
        body = extract_caml('Bill')

        if body == '':
            # If there isn't a caml:Bill tag, then there must
            # be a caml:Content tag.
            body = extract_caml('Content')
        try:
            dd_cursor.execute(QU_BILL_VERSION, (title, digest, body, STATE, vid))
            UPDATE += dd_cursor.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert Failed for BillVersion',
                                                            (QU_BILL_VERSION % (title, digest, body, STATE, vid))))

if __name__ == "__main__":
    with connect() as dd_cursor:
        with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                             user='monty',
                             db='capublic',
                             passwd='python',
                             #host='localhost',
                             #user='root',
                             #db='historic_capublic',
                             #passwd='',
                             charset='utf8') as ca_cursor:
            logger = create_logger()
            billparse(ca_cursor, dd_cursor)
    LOG = {'tables': [{'state': 'CA', 'name': 'BillVersion', 'inserted':0 , 'updated': UPDATE, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))
    logger.info(LOG)