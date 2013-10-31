from datetime import datetime

from catflap.dao import DomainObject as DomainObject
from catflap.dao import log

'''
Define models in here. They should all inherit from the DomainObject.
Look in the dao.py to learn more about the default methods available to
the Domain Object.
'''
class Journal(DomainObject):
    __type__ = 'journal'

    # A journal is identified by one of these fields. When a record is
    # about to be indexed, this class will check whether a record about
    # the journal being added already exists. However, a publisher name
    # alone is insufficient to identify a distinct journal - a name, an
    # ISSN, something else is necessary. Those are the identity fields,
    # and one of them *has* to be specified when a record is getting
    # added via catflap.api .
    identity_fields = ['issn', 'print_issn', 'electronic_issn',
            'journal_title', 'journal_abbreviation']

    # all the info we keep about a journal which is held in lists
    # FIXME get up-to-date information on which fields are arrays from
    # the ES mapping, put these as arrays in the default mapping
    list_fields = identity_fields + ['publisher_name']

    # normal fields
    single_value_fields = []

    # all the fields
    fields = list_fields + single_value_fields

    def __init__(self, **kwargs):

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
        
        # for the moment we are going to not bother with provenance
        # much more time thinking about the consequences of it and how it's going to
        # be used are required
        if "source" in instance.data:
            del instance.data["source"]
        # instance.generate_provenance()
        
        if not instance.propagate():
            instance.save()

    @classmethod
    def search(cls, size=10, **kwargs):
        instance = cls(**kwargs)
        return instance.find(size=size)
    
    def generate_provenance(self):
        # get the supplied source field, and then remove it from the object's data
        source = self.data.get("source", "unknown")
        if "source" in self.data:
            del self.data["source"]
        
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # build a provenance object describing each field
        # e.g. {"issn" : "1234-5678", "source" : "pubmed/12345678", "date" : "<yesterday>"},
        provenance = []
        for k, v in self.data.items():
            if isinstance(v, list):
                for val in v:
                    p = {k : val, "source" : source, "date" : timestamp}
                    provenance.append(p)
            else:
                p = {k : v, "source" : source, "date" : timestamp}
                provenance.append(p)
        
        # attach the provenance to the item
        self.data["provenance"] = provenance
    
    def find(self, similar=False, size=10):
        # FIXME the similar parameter is not used anywhere in this
        # method
        terms = {}

        for k,v in self.data.items():
            if v and k != "provenance" and k in self.identity_fields:
                # do not include None-s or the provenance
                # also, only look for fields which identify the journal,
                # not any old field the object happens to contain (like
                # publisher_name)
                terms[k + '.exact'] = v

        r = self.query(terms=terms, terms_operator="should", size=size)

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
            dosave = False
            for field,val in self.data.items():
                if field != "provenance":
                    # we can only merge lists of things which are primitives
                    newdata, journal[field] = self.make_merge_list(journal.data.get(field, []), val)
                    if newdata:
                        dosave = True
                        
                # turning provenance handling off for the time being
                #else:
                #    # provenance is a list of dicts, and we don't want to deduplicate it anyway
                #    journal[field] += val
                
            if dosave:
                print "saving ", journal.id
                journal.save()
            else:
                print "not saving ", journal.id

        return True

    @staticmethod
    def make_merge_list(target, source):
        '''
        All arguments are added to a list. If one of the arguments is a
        list itself, then its elements get appended to the result (no
        nested lists).
        '''
        # make sure we are dealing with lists
        if not isinstance(target, list):
            target = [target]
        if not isinstance(source, list):
            source = [source]
        
        newdata = False
        results = []
        for s in source:
            if s not in target:
                target.append(s)
                newdata = True
        
        return newdata, target

class Provenance(DomainObject):
    __type__ = "provenance"

def check_list(l):
    '''Make sure a list doesn't just have None elements.'''
    tmp = [i for i in l if i]

    if tmp:
        return True
    else:
        return False
