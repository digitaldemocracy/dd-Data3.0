#!/bin/bash

export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_legislators.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_committeeauthors.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_bills.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_authors.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_actions.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/NY-Build/ny_import_agendas.py 

