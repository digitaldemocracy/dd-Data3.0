#!/bin/bash

export SCRIPTPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/
export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_legislators.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_committees.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_bills.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_authors.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_hearings.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX/tx_import_contributions.py -q
