import sys
from catflap.models import Journal

FIX_FIELD = 'journal_title'
strange = None  # log strange things to this file handle


def fix(q=None):
    # TODO do this using the scroll API http://www.elasticsearch.org/guide/reference/api/search/search-type/
    everything = Journal.query(q=q, size=10000000)
    processed = 0

    try:
        if everything['hits']['total'] <= 0:
            raise Exception('Nothing to fix')
    except KeyError:
        print 'ES returned a strange result, probably an error. Is ' \
              'your index missing? Here is the original response:' \
              '\n ', everything

    for record in everything['hits']['hits']:
        processed += 1
        instance = Journal(**record['_source'])

        if FIX_FIELD not in instance:
        # The field we're fixing is not in this record. Move on.
            processed -= 1
            continue

        if not isinstance(instance[FIX_FIELD], list):
            strange.write(instance['id'] + ' - Journal title not a '
                                           'list' + '\n')
            processed -= 1
            continue

        new_titles = []
        for t in instance[FIX_FIELD]:
            # fix 1 - flatten nested lists
            if isinstance(t, list):
                for subitem in t:
                    if subitem not in new_titles:
                        new_titles.append(subitem)
                continue

            # fix 2 - remove values which are just True
            if t is not True:  # identity check - this will only be
                               # true if if t is not the Python boolean
                               # object True. E.g.:
                               # - 'la', 1, False, "True" are all OK
                               # - True is not OK
                new_titles.append(t)

        instance[FIX_FIELD] = new_titles
        instance.save()
    Journal.refresh()
    return processed


if __name__ == '__main__':
    strange = open(sys.argv[0] + '_strange.log', 'ab')
    if len(sys.argv) == 2:
        processed = fix(sys.argv[1])
    else:
        processed = fix()

    print 'Processed {0} records. Done.'.format(processed)
    strange.close()
