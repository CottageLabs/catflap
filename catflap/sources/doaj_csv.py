from copy import copy
from datetime import datetime
import re
import csv
import requests
from StringIO import StringIO

from catflap import Journal

DOAJ_CSV_URL = 'http://www.doaj.org/doaj?func=csv'
_data = []

# the CSV columns
_TITLE = 0
_TITLE_ALT = 1
_PUBLISHER = 3
_ISSN = 5
_EISSN = 6

# The following problematic values in the CSV must be ignored:
# 1/ Certain fields contain '-' as a way to record the field has no
# value.
# 2/ Some fields contain just whitespace
_NO_VAL = re.compile('^\s*-*\s*$')

def get_doaj_csv():
    '''Returns a Unicode string containing the DOAJ CSV data.'''
    r = requests.get(DOAJ_CSV_URL)
    # auto-detected encoding is latin1, breaks umlauts and other chars
    # setting r.encoding makes requests use that encoding when
    # constructing r.text from the original request data, i.e. forces
    # the encoding so we get our umlauts.
    r.encoding = 'utf-8'
    return r.text

def load_csv(s):
    '''Load CSV data into _data from a string in memory (s).'''
    # need to convert the unicode string into a normal string here
    # since reading rows from csvreader (below) forces a conversion
    # attempt using the 'ascii' codec, and we need to use 'utf-8',
    # so better do it ourselves
    csvreader = csv.reader(StringIO(s.encode('utf-8')))
    csvreader.next()  # we do not need the header
    for row in csvreader:
        newrow = []
        for field in row:
            if not _NO_VAL.match(field):
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
        if row[_TITLE_ALT] and row[
            _TITLE_ALT] not in jnames: jnames.append(row[_TITLE_ALT])

        eissn = []
        if row[_EISSN]: eissn.append(row[_EISSN])

        publisher = []
        if row[_PUBLISHER]: publisher.append(row[_PUBLISHER])

        Journal.add(issn=issns, electronic_issn=eissn,
                    journal_title=jnames, publisher_name=publisher)


if __name__ == '__main__':
    print 'Loading data from ' + DOAJ_CSV_URL
    load_csv(get_doaj_csv())
    print 'Indexing data start ' + datetime.now().isoformat()
    index()
    print 'Indexing data end ' + datetime.now().isoformat()
    print 'Processed {0} records. Done.'.format(len(_data))
