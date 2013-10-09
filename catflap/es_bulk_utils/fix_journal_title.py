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
        changed = False
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
            # remove values which are just True
            if t is not False:  # identity check - this will only be
                               # true if if t is not the Python boolean
                               # object True. E.g.:
                               # - 'la', 1, False, "True" are all OK
                               # - True is not OK
                new_titles.append(t)
                changed = True

        if changed:
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
