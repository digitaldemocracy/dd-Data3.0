#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
'''
File: billparse.py
Author: ???
Modified By: Daniel Mangin, Mandy Chan, Steven Thon
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

import loggingdb
from lxml import etree 
import MySQLdb
import re

# U.S. State
STATE = 'CA'

# Queries
QS_CPUB_BILL_VERSION = '''SELECT bill_version_id, bill_xml
                          FROM bill_version_tbl'''
QU_BILL_VERSION = '''UPDATE BillVersion
                     SET title = %s, digest= %s, text = %s, state = %s
                     WHERE vid = %s'''

def get_bill_versions(ca_cursor):
  ca_cursor.execute(QS_CPUB_BILL_VERSION)
  for vid, xml in ca_cursor.fetchall():
    # IS THIS OKAY??
    if xml is None:
      continue
    xml = xml.strip()
    flags = re.DOTALL
    xml = re.sub(r'<\?xm-(insertion|deletion)_mark\?>', r'', xml, flags)
    xml = re.sub(r'<\?xm-(insertion|deletion)_mark (?:data="(.*?)")\?>',
        r'<span class="\1">\2</span>', xml, flags)

    # These 2 lines give problems. Don't need them for now, might in the future...
    #xml = re.sub(r'<\?xm-(insertion|deletion)_mark_start\?>', r'<span class="\1">', xml, flags)
    #xml = re.sub(r'<\?xm-(insertion|deletion)_mark_end\?>', r'</span>', xml, flags)
    yield '%s_%s' % (STATE, vid), xml

def billparse(ca_cursor, dd_cursor):
  for vid, xml in get_bill_versions(ca_cursor):
    root = etree.fromstring(xml)
    namespace = {'caml':'http://lc.ca.gov/legalservices/schemas/caml.1#'}

    # Get title
    title = root.xpath('//caml:Title', namespaces=namespace)[0].text

    # Get digest
    digest_nodes = root.xpath('//caml:DigestText', namespaces=namespace)
    digest = digest_nodes[0].text if len(digest_nodes) > 0 else ''

    # Get body
    body_nodes = root.xpath('//caml:Bill', namespaces=namespace)
    if len(body_nodes) > 0:
      body = body_nodes[0].text
    else:
      # If there isn't a caml:Bill tag, then there must
      # be a caml:Content tag.
      #pat = '<{0}Content>.*?</{0}Content>'.format(namespace['caml'])
      pat = '<{0}:Content>.*?</{0}:Content>'.format('caml')
      body = ''.join(node for node in re.findall(pat, xml, re.DOTALL))

    dd_cursor.execute(QU_BILL_VERSION, (title, digest, body, vid, STATE))

if __name__ == "__main__":
  # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789',
                         charset='utf8') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='capublic',
                         passwd='python',
                         charset='utf8') as ca_cursor:
      billparse(ca_cursor, dd_cursor)
