The files in this project scrape support and opposition information from bill analysis files for
California. That information is loaded into the OrgAlignments table in the database. From here the
info is processed and and loaded into the tables necessary for the alignment meter. Note
that all scripts run sloowly

Process for Running: (In order)

Run: ScrapeSupportFixed.py
    - Downloads data in lob files from here: 'http://downloads.leginfo.legislature.ca.gov/pubinfo_2017.zip'
    - Unzips these files into BillAnalysisLobs
    - Writes csv files containing support and opposition into BillAnalysisOut

Run: InsertAlignments.py
    - Reads the data from the csvs in BillAnalysisOut and writes that info to
      the database

Run: AlignmentMeter.py
    - This aggregates info in org alignments and creates the relevant information for the
      alignment meter


ALTERNATIVELY:

- Run AlignmentMeterDriver.py. This simply calls all these files


Testing:
AlignmentMeterTests.py contains unit tests for AligmentMeter.py. If you make any changes please
run these tests and make sure they pass to ensure that you did not break anything upstream

SetUpTests.ipynb is a Python notebook which has some independent tests I wrote earlier. You may find it
helpful to run through these and inspect how the output should look.