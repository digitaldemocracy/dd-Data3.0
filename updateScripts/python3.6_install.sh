#!/bin/bash

#Python install dependencies
yum -y groupinstall development
yum -y install zlib-devel

#download python 3.6
wget https://www.python.org/ftp/python/3.6.3/Python-3.6.3.tar.xz
tar xJf Python-3.6.3.tar.xz
cd Python-3.6.3
./configure
make
make install

#print interpreter location and version
which python3
python3 -V

#modules needed for import scripts
python3.6 -m pip install beautifulsoup4
python3.6 -m pip install bs4
python3.6 -m pip install certifi
python3.6 -m pip install chardet
python3.6 -m pip install idna
python3.6 -m pip install lxml
python3.6 -m pip install mysqlclient
python3.6 -m pip install pip
python3.6 -m pip install pytz
python3.6 -m pip install requests
python3.6 -m pip install setuptools
python3.6 -m pip install six
python3.6 -m pip install urllib3