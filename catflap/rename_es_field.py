"""
Rename a field in all indexed documents in the acat index.
Usage:
python rename_es_field.py <source name> <destination name>
    [elasticsearch query string]

<source name> will be deleted after its contents are copied to
<destination name>, for every document that is processed.

You can optionally pass in [elasticsearch query string] in order to
determine which documents should be processed. The value is whatever
you would  pass to elasticsearch via the HTTP GET API in the "q"
parameter:

    http://localhost:9200/acat/journal/_search?q=[elasticsearch query string]

Alternatively, from a python script or the python interpreter:
from catflap import rename_es_field
rename_es_field.rename_field(source_name, destination_name)

or

rename_es_field.rename_field(source_name, destination_name,
    elasticsearch_query_string)
"""

import sys
from models import Journal


def rename_field(src, dst, q=None):
    # TODO do this using the scroll API http://www.elasticsearch.org/guide/reference/api/search/search-type/
    everything = Journal.query(q=q, size=10000000)

    try:
        if everything['hits']['total'] <= 0:
            raise Exception('Nothing to rename')
    except KeyError:
        print 'ES returned a strange result, probably an error. Is ' \
              'your index missing? Here is the original response:' \
              '\n ', everything

    for record in everything['hits']['hits']:
        instance = Journal(**record['_source'])
        # if both source and destination are present, merge them as
        # a list
        if src in instance and dst in instance:
            instance[dst] = Journal.make_merge_list(
                instance[dst], instance[src]
            )
        else:
            instance[dst] = instance[src]
        del instance[src]  # delete original field
        instance.save()
    Journal.refresh()


if __name__ == '__main__':
    if len(sys.argv) == 3:  # only source and destination of rename
        rename_field(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:  # also includes ES query string,
                              # apply rename only to its results
        rename_field(sys.argv[1], sys.argv[2], sys.argv[3])