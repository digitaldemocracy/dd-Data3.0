#!/usr/bin/env python
'''
File: TSV_extract.py
Author: Matt Versaggi
Modified By: N/A
Last Modified: January 26, 2016

Description:
    - Downloads dbwebexport.zip (CAL-ACCESS raw data) from the Secretary of
        State website and extracts the CVR_REGISTRATION_CD.TSV file from it
        for use by Cal-Access-Accessor.py
    - The extracted .TSV is placed in the same directory as the one this 
        script is run from
    - This script runs under the update script before Cal-Access-Accessor.py

Sources:
    - http://www.sos.ca.gov/campaign-lobbying/cal-access-resources/raw-data-campaign-finance-and-lobbying-activity (Website)
    - http://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip (Zip)
'''

import os
import subprocess
import sys
import zipfile

zipURL = "http://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"
tsvPath = "CalAccess/DATA/CVR_REGISTRATION_CD.TSV"
zipName = "dbwebexport.zip"

'''
Retrieves dbwebexport.zip and extracts the .TSV file from it
'''
def get_zip():
    # Downloads the zip and places it in pwd of this script
    wget_process = subprocess.Popen("wget -t 10 " + zipURL,
                                    shell=True,
                                    stderr=subprocess.PIPE)
    _, err = wget_process.communicate()
    if wget_process.returncode != 0:
      #sys.stderr.write(err)
      sys.exit(0)
    
    calZip = zipfile.ZipFile(zipName, 'r')
    calZip.extract(tsvPath)
    calZip.close()

'''
Pulls the .TSV out of the CalAccess directory tree and places it in the pwd,
    removing the empty directory tree and dbwebexport.zip before returning
'''
def cleanup():
    subprocess.call("mv " + tsvPath + " .", shell=True)
    subprocess.call("rm -r CalAccess", shell=True)
    subprocess.call("rm -f " + zipName, shell=True)

def main():
    os.chdir('/home/data_warehouse_common/scripts')
    get_zip()
    cleanup()

if __name__ == '__main__':
    main()
