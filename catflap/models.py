from datetime import datetime

from catflap.dao import DomainObject as DomainObject

'''
Define models in here. They should all inherit from the DomainObject.
Look in the dao.py to learn more about the default methods available to
the Domain Object.
'''
class Journal(DomainObject):
    __type__ = 'journal'

    # all the info we keep about a journal
    fields = ['issn', 'journal_name', 'publisher']

    #def __init__(self, **kwargs):
    #    Journal.check_journal_data(**kwargs)

    #    # all journal data fields specified above should be lists
    #    for field in self.fields:
    #        if field in kwargs:
    #            if not isinstance(kwargs[field], list):
    #                kwargs[field] = [kwargs[field]]

    #    DomainObject.__init__(self, **kwargs)
    
    @classmethod
    def index(cls, issn=None, journal_name=None, publisher=None):
        instance = cls(issn=[issn], journal_name=[journal_name],
                publisher=[publisher])
        if not instance.propagate():
            instance.save()

    @classmethod
    def search(cls, issn=None, journal_name=None, publisher=None):
        instance = cls(issn=[issn], journal_name=[journal_name],
                publisher=[publisher])
        return instance.find()
        
    def find(self, similar=False):
        terms = {}

        for field in self.fields:
            terms[field] = self.data[field]

        r = self.query(terms=terms, terms_operator="should")

        if 'hits' not in r:
            return None

        r = r['hits']

        if r['total'] <= 0:
            return None

        results = []
        for hit in r['hits']:
            results.append(Journal(**hit))
        
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
        for journal in journals_like_me[:] :
            if journal.data['id'] == self.data['id']:
                journals_like_me.remove(journal)

        if not journals_like_me:
            return False

        for journal in journals_like_me:
            for field in self.fields:

                journal.data[field] = self.merge_lists(self.data[field],
                        journal.data[field])

            journal.save()

        return True

    @staticmethod
    def merge_lists(src, dst):
        '''
        All elements in src which are not in dst are copied to dst. Original
        dst list not modified.
    
        Returns the modified version of dst.
        '''
        new_dst = dst[:]
        for item in src:
            if item not in dst:
                new_dst.append(item)
    
        return new_dst

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
            try:
                data[field] = kwargs[field]
                ok = True # at least one field must be present
            except KeyError:
                pass

        if not ok:
            raise ValueError('You need to provide at least one piece \
of information to create or find a Journal object.')

        return data
