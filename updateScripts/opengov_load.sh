#!/bin/bash

DATA=/home/mchan18/dd-Data3.0/updateScripts/leginfo_load
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "Deleting capublic data..."
for j in capublic; \
do for i in `echo 'show tables ' |mysql -uroot $j \
|grep -v 'Tables_in'`; do mysql -uroot $j -e "truncate $i"; done; done

mkdir $DATA 2> /dev/null

for fold in $@
do
   echo "$fold"

   if [ ! -e $fold ]; then
      mkdir $fold
      cd $fold
      wget ftp://www.leginfo.ca.gov/pub/bill/$fold.zip --no-check-certificate 
      unzip $fold.zip
      rm $fold.zip
      cd ..
   fi

   echo "Moving bill version data"

   `rm $DATA/* 2> /dev/null`
   `cp $fold/BILL_VERSION*.lob $DATA`

   echo "Applying bill version data permissions"

   chown -R mysql $DATA
   chmod -R 777 $DATA
   `chmod -R 777 $DATA/*.lob`

   for TBL in ~/dd-Data3.0/updateScripts/opengov_load/*
   do
      echo "   Loading ${TBL%.*}..."
      (cd $fold && mysql -uroot --local-infile=1 -Dcapublic -f -v < $TBL)
   done
   rm -rf $fold
done
