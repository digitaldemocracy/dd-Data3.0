#!/bin/bash

export SCRIPTPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/
export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_bills.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_bill_parse.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_action.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_authors.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_committees.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_motion.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_vote.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/TSV_extract.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/ca_import_lobbyists.py -q
