from copy import copy
from datetime import datetime
import re
import csv

from catflap import Journal

DOAJ_CSV_PATH = '/home/emanuil/cl/'
DOAJ_CSV_FN = 'doaj_20130924_2334.csv'
DOAJ_CSV = DOAJ_CSV_PATH + DOAJ_CSV_FN
_data = []

# the CSV columns
_TITLE = 0
_TITLE_ALT = 1
_PUBLISHER = 3
_ISSN = 5
_EISSN = 6

# Certain fields contain '-' as a way to record the field has no value.
_NO_VAL = re.compile('^\s*-\s*$')

def load_csv(filename):
    _data = []
    with open(filename, 'rb') as csvfile:
        i = csv.reader(csvfile)
        for row in i:
            newrow = []
            for field in row:
                if not no_val.match(field):
                    newrow.append(field)
                else: # keep the same number of columns
                    newrow.append('')
            _data.append(copy(newrow))

def index():
    for row in _data:
        issns = []
        if row[_ISSN]: issns.append(row[_ISSN])
        if row[_EISSN]: issns.append(row[_EISSN])

        jnames = []
        if row[_TITLE]: jnames.append(row[_TITLE])
        if row[_TITLE_ALT] and row[_TITLE_ALT] not in jnames: jnames.append(row[_TITLE_ALT])

        eissn = []
        if row[_EISSN]: eissn.append(row[_EISSN])

        publisher = []
        if row[_PUBLISHER]: eissn.append(row[_PUBLISHER])

        Journal.add(issn=issns, journal_name=jnames, publisher_name=publisher, source=DOAJ_CSV_FN)

if __name__ == '__main__':
    print 'Loading data from ' + DOAJ_CSV
    load_csv(DOAJ_CSV)
    print 'Indexing data start ' + datetime.now.isoformat()
    index()
    print 'Indexing data end ' + datetime.now.isoformat()
    print 'Done.'
