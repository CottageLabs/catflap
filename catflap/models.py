from datetime import datetime

from catflap.dao import DomainObject as DomainObject

'''
Define models in here. They should all inherit from the DomainObject.
Look in the dao.py to learn more about the default methods available to
the Domain Object.
'''
class Journal(DomainObject):
    __type__ = 'journal'

    # all the info we keep about a journal which is held in lists
    list_fields = ['issn', 'journal_title', 'journal_abbreviation',
            'publisher_name']

    # normal fields
    single_value_fields = []

    # all the fields
    fields = list_fields + single_value_fields



    def __init__(self, **kwargs):
        Journal.check_journal_data(**kwargs)

        # All fields specified in Journal.list_fields should be lists.
        # We might often pass single values for list fields when adding
        # or searching for a journal, e.g. just pass issn="0000-1111".
        # This needs to become "issn": ["0000-1111"] in order to be
        # treated properly though.
        for field in self.list_fields:
            if field in kwargs:
                if not isinstance(kwargs[field], list):
                    kwargs[field] = [kwargs[field]]

        DomainObject.__init__(self, **kwargs)
    
    @classmethod
    def index(cls, **kwargs):
        instance = cls(**kwargs)
        if not instance.propagate():
            instance.save()

    @classmethod
    def search(cls, **kwargs):
        instance = cls(**kwargs)
        return instance.find()
        
    def find(self, similar=False):
        terms = {}

        for k,v in self.data.items():
            if v: # do not include None-s
                terms[k + '.exact'] = v

        r = self.query(terms=terms, terms_operator="should")

        if 'hits' not in r:
            return None

        r = r['hits']

        if r['total'] <= 0:
            return None

        results = []
        for hit in r['hits']:
            results.append(Journal(**hit['_source']))
        
        return results

    def propagate(self):
        journals_like_me = self.find(similar=True)

        if not journals_like_me:
            return False

        # remove self from list of results, only want similar ones -
        # without this exact record
        # TODO add an argument to self.find() which modifies the terms
        # or builds a different query to exclude the object with id ==
        # self.data['id']
        #for journal in journals_like_me[:] :
        #    if journal.data['id'] == self.data['id']:
        #        journals_like_me.remove(journal)

        #if not journals_like_me:
        #    return False

        for journal in journals_like_me:
            for field,val in self.data.items():
                journal[field] = self.make_merge_list(
                        val,
                        journal.data.get(field) # journal.get is a
                        # different method from the dictionary .get
                        )

            journal.save()

        return True

    @staticmethod
    def make_merge_list(*args):
        '''
        All arguments are added to a list. If one of the arguments is a
        list itself, then its elements get appended to the result (no
        nested lists).
        '''

        results = []
        for a in args:
            if isinstance(a, list):
                results.extend(a)
            else:
                results.append(a)

        return list(set(results))

    @classmethod
    def check_journal_data(cls, **kwargs):
        '''
        Check a keyword arguments dict for journal data. At least one of
        the which has to be present. If none are present, it raises a
        ValueError.

        Returns a dict containing only the required journal data.
        '''
        data = {}
        ok = False

        for field in cls.fields:
            debug_msg = field + ': '
            try:
                data[field] = kwargs[field]
                if not isinstance(data[field], list):
                    debug_msg += 'not a list, ok'
                    ok = True
                else:
                    debug_msg += 'is a list, '
                    if cls.check_list(data[field]):
                        debug_msg += 'ok'
                        ok = True # at least one field must be present
                    else:
                        debug_msg += 'NOT ok'
            except KeyError:
                debug_msg += 'does not exist'
                pass

        if not ok:
            log.debug(debug_msg)
            raise ValueError('You need to provide at least one piece \
of information to create or find a Journal object.')

        return data

    @staticmethod
    def check_list(l):
        '''Make sure the list doesn't just have None elements.'''
        tmp = [i for i in l if i]

        if tmp:
            return True
        else:
            return False
