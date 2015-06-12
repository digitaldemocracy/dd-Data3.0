#!/bin/bash

DATA=/home/dmangin/dd-Data3.0/updateScripts/leginfo_wed
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

mkdir $DATA 2> /dev/null

for fold in $@
do
   echo "$fold"

   if [ ! -e $fold ]; then
      mkdir $fold
      cd $fold
      wget ftp://www.leginfo.ca.gov/pub/bill/$fold.zip 
      unzip $fold.zip
      rm $fold.zip
      cd ..
   fi

   echo "Moving bill version data"

   rm $DATA/* 2> /dev/null
   cp $fold/BILL_VERSION*.lob $DATA

   echo "Applying bill version data permissions"

   chown -R mysql $DATA
   chmod -R 777 $DATA
   chmod -R 777 $DATA/*.lob

   for TBL in opengov_load/*
   do
      echo "   Loading ${TBL%.*}..."
      (cd $fold && mysql -uroot --local-infile=1 -Dcapublic -f -v < $DIR/$TBL)
   done
   rm -rf $fold
done
