#!/bin/bash

# bash script to download bulk tsv files from NEISS archive
# saves to local folder data/, which is later read by the python parser
# skips files that already exist

mkdir -p data/
for year in $(seq 1999 2018); do
    
    f=data/neiss$year.tsv
    
    if [ -f "$f" ]; then
        echo "$f exists"
    else
        echo downloading $year
        wget -N -q https://www.cpsc.gov/cgibin/NEISSQuery/Data/Archived%20Data/$year/neiss$year.tsv -O data/neiss$year.tsv
        echo $f;
    fi
    
done