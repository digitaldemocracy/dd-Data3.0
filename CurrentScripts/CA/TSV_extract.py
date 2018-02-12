#!/usr/bin/env python
'''
File: TSV_extract.py
Author: Matt Versaggi
Modified By: N/A
Last Modified: February 5, 2016
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

import zipfile
import subprocess
import contextlib
import os
from datetime import datetime

zipURL = "http://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"
tsvPath = "CalAccess/DATA/CVR_REGISTRATION_CD.TSV"
zipName = "dbwebexport.zip"

'''
Attempts to download dbwebexport.zip and extract the .TSV file from it,
    logging the result in TSV_extract_log.txt
If successful, continues with extracting the .TSV and cleaning up after.
If unsuccessful, removes the partial zip (if existant) and records return code
'''
def get_zip():
    # Attempts to quietly download the zip, giving up after 10 attempts
    returnCode = subprocess.call("wget -q -t 10 " + zipURL, shell=True)

    with open("TSV_extract_log.txt", 'a') as logFile:
        if returnCode == 0:
            logFile.write("On {0} at {1}: TSV download successful\n"
                          .format(datetime.date(datetime.now()),
                                  datetime.time(datetime.now())))
            with contextlib.closing(zipfile.ZipFile(zipName, 'r')) as calZip:
                calZip.extract(tsvPath)
            cleanup()
        else:
            logFile.write("On {0} at {1}: TSV download failed, returned wget error code {2}\n"
                          .format(datetime.date(datetime.now()),
                                  datetime.time(datetime.now()), returnCode))
    if os.path.exists("./" + zipName):
        subprocess.call("rm -f " + zipName, shell=True)
        
'''
Pulls the .TSV out of the CalAccess directory tree and places it in the pwd,
    removing the empty directory tree before returning
'''
def cleanup():
    subprocess.call("mv " + tsvPath + " .", shell=True)
    subprocess.call("rm -r CalAccess", shell=True)

def main():
    get_zip()

if __name__ == '__main__':
    os.chdir('/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/')
    main()
