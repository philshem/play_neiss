**work in progress**

# play_neiss

Download and parsing and merging tool for the raw data from the [National Electronic Injury Surveillance System]((https://www.cpsc.gov/Research--Statistics/NEISS-Injury-Data)) (NEISS)

> The primary purpose of NEISS is to collect data on consumer product-related injuries occurring in the United States. This data is what CPSC uses to produce nationwide estimates of product-related injuries.

## get the raw data

Bash script to download NEISS raw data from the [query tool](https://www.cpsc.gov/cgibin/NEISSQuery/home.aspx)

## parse the raw data

Python3.7 script to map metadata, and output csv, json, pickle, or sqlite3 database. Choose your output option(s) by setting these flags in the top of the Python code:

```
# export options
csv_tf = True #.csv, default
csv_sep = '\t'
db_tf = False #.sl3 sqlite3 db
pickle_tf = False #.pkl pickle of pandas dataframe
json_tf = False #.json
# file name and path
prefix = 'output'+os.sep+'neiss'
```

## notes

The (un)licence applies to only the code. I do not have any claim to license the data.

### validate the raw data

It can be that some rows have the wrong number of lines. Use a tool like [csvlint](https://github.com/Clever/csvlint) if you want to validate the csv files.

    for f in data/*.tsv; do
        echo $f
        awk -F'\t' ' { print NF }' $f | awk '{ total += $1; count++ } END { print total/count " average columns per file"}'
        ./csvlint -delimiter '\t' $f
        echo "***********************************\n\n"
    done
