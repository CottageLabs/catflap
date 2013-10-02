'''
Rename a field in all indexed documents in the acat index.
Usage:
python rename_es_field.py <source name> <destination name>

<source name> will be deleted after its contents are copied to
<destination name>, for every document that is processed (all of them
by default).

Alternatively, from a python script or the python interpreter:
from catflap import rename_es_field
rename_es_field.rename_field(source_name, destination_name)
'''
import json

import sys
import requests
from models import Journal


def rename_field(src, dst):
    # TODO do this using the scroll API http://www.elasticsearch.org/guide/reference/api/search/search-type/
    everything = Journal.query(size=10000000)

    try:
        if everything['hits']['total'] <= 0:
            raise Exception('Nothing to rename')
    except KeyError:
        print 'ES returned a strange result, probably an error. Is ' \
              'your index missing? Here is the original response:' \
              '\n ', everything

    for record in everything['hits']['hits']:
        instance = Journal(**record['_source'])
        instance[dst] = instance[src]
        del instance[src]
        instance.save()
    Journal.refresh()


if __name__ == '__main__':
    rename_field(sys.argv[1], sys.argv[2])