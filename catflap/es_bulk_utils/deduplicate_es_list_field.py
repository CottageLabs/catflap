import sys
from catflap.models import Journal

strange = None  # log strange things to this file handle


def deduplicate(fname, q=None):
    # TODO do this using the scroll API http://www.elasticsearch.org/guide/reference/api/search/search-type/
    everything = Journal.query(q=q, size=10000000)
    processed = 0

    try:
        if everything['hits']['total'] <= 0:
            raise Exception('Nothing to deduplicate')
    except KeyError:
        print 'ES returned a strange result, probably an error. Is ' \
              'your index missing? Here is the original response:' \
              '\n ', everything

    for record in everything['hits']['hits']:
        instance = Journal(**record['_source'])

        if fname not in instance:
        # The field we're deduplicating is not in this record. Move on.
            continue

        if not isinstance(instance[fname], list):
            strange.write(instance['id'] + ' - ' + fname + ' not a '
                                           'list' + '\n')
            continue

        before = len(instance[fname])
        instance[fname] = list(set(instance[fname]))  # deduplicate
        after = len(instance[fname])

        if before != after:
            instance.save()
            processed += 1

    Journal.refresh()
    return processed


if __name__ == '__main__':
    strange = open(sys.argv[0][:-3] + '_strange.log', 'ab')
    if len(sys.argv) == 2:
        processed = deduplicate(fname=sys.argv[1])
    elif len(sys.argv) == 3:
        processed = deduplicate(fname=sys.argv[1], q=sys.argv[2])

    print 'Processed {0} records. Done.'.format(processed)
    strange.close()
