#!/bin/bash

sudo apt-get install python python-setuptools

# installed editdist module from http://py-editdist.googlecode.com/files/py-editdist-0.3.tar.gz
wget http://py-editdist.googlecode.com/files/py-editdist-0.3.tar.gz
tar -xzvf py-editdist-0.3.tar.gz
cd py-editdist-0.3/
python setup.py build
sudo python setup.py install

sudo apt-get install python-sklearn
