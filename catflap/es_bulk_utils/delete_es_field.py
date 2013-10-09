"""
Delete a field in all indexed documents in the acat index.
Usage:
python delete_es_field.py <field name> [elasticsearch query string]

<field name> will be deleted from every document that is processed.

You can optionally pass in [elasticsearch query string] in order to
determine which documents should be processed. The value is whatever
you would  pass to elasticsearch via the HTTP GET API in the "q"
parameter:

    http://localhost:9200/acat/journal/_search?q=[elasticsearch query string]

Alternatively, from a python script or the python interpreter:
from catflap import delete_es_field
delete_es_field.delete_field(field_name)

or

delete_es_field.delete_field(field_name, elasticsearch_query_string)
"""

import sys
from catflap.models import Journal


def delete_field(name, q=None):
    # TODO do this using the scroll API http://www.elasticsearch.org/guide/reference/api/search/search-type/
    everything = Journal.query(q=q, size=10000000)

    try:
        if everything['hits']['total'] <= 0:
            raise Exception('Nothing to delete')
    except KeyError:
        print 'ES returned a strange result, probably an error. Is ' \
              'your index missing? Here is the original response:' \
              '\n ', everything

    for record in everything['hits']['hits']:
        instance = Journal(**record['_source'])

        if name not in instance:
        # The field we're deleting is not in this record. Move on.
            continue

        del instance[name]
        instance.save()
    Journal.refresh()
    return everything['hits']['total']


if __name__ == '__main__':
    if len(sys.argv) == 2:  # only the field to delete
        processed = delete_field(sys.argv[1])
    elif len(sys.argv) == 3:  # also includes ES query string,
                              # apply delete only to its results
        processed = delete_field(sys.argv[1], sys.argv[2])
    print 'Processed {0} records. Done.'.format(processed)