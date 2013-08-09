from catflap.models import Journal as JournalModel

class Journal:
    @staticmethod
    def add(**kwargs):
        return JournalModel.index(**kwargs)

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
