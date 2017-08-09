#!/bin/bash

export SCRIPTPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/
export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_legislators.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_committeeauthors.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_bills.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_authors.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_actions.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_agendas.py -q &&
