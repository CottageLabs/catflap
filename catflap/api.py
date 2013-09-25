from catflap.models import Journal as JournalModel

class Journal:
    @staticmethod
    def add(issn=None, print_issn=None, electronic_issn=None,
            journal_title=None, journal_abbreviation=None, **kwargs):
        d = {}
        d['issn'] = issn; d['print_issn'] = print_issn; d['electronic_issn'] = electronic_issn;
        d['journal_title'] = journal_title; d['journal_abbreviation'] = journal_abbreviation;

        required = d.keys()
        ok = False
        
        for k,v in d.items():
            if v:
                ok = True
            else:
                # don't pass on None-s
                del d[k] # safe, iterating over a copy of the items in d
        if not ok:
            raise ValueError('You have to provide a value for at least one of these keyword args: ' + required)

        d.update(kwargs) # you can't repeat keyword args (SyntaxError) so no chance of overwriting values here

        JournalModel.index(**d)
        
        # we need to refresh so that bulk loading operations can access the current data straight away
        JournalModel.refresh()

    @staticmethod
    def search(**kwargs):
        return JournalModel.search(**kwargs)

def init():
    JournalModel.initialise_index()

init() # yes, even when imported. FIXME. Current alternative is to call
       # this each time JournalModel is asked to do something, which is
       # unnecessary. There are better ways, e.g. get the mapping and
       # compare it to the one in the config, though when would you do
       # that comparison? Still on import..?
