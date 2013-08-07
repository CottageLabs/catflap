from catflap.models import Journal as JournalModel

class Journal:
    @staticmethod
    def add(issn=None, journal_name=None, publisher=None):
        return JournalModel.index(issn=issn, journal_name=journal_name,
                publisher=publisher)

    @staticmethod
    def search(issn=None, journal_name=None, publisher=None):
        return JournalModel.search(issn=issn, journal_name=journal_name,
                publisher=publisher)
