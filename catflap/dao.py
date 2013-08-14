import os, json, UserDict, requests, uuid, logging
from simplejson import JSONDecodeError

from datetime import datetime

from catflap import config

'''
All models in models.py should inherit this DomainObject to know how to
save themselves in the index and so on.  You can overwrite and add to
the DomainObject functions as required. See models.py for some examples.
'''

LOG_FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
log = logging.getLogger(__name__)

class DomainObject(UserDict.IterableUserDict):
    __type__ = None # set the type on the model that inherits this
    base_index_url = 'http://' + str(config.ELASTIC_SEARCH_HOST).lstrip('http://').rstrip('/') + '/' + config.ELASTIC_SEARCH_DB

    def __init__(self, **kwargs):
        '''
        Can be constructed using the results of elasticsearch queries or by
        passing any dictionary.
    
        The contents of any dictionary which does not look like an
        elasticsearch result (does not have the '_source' key) will be
        copied to the new DomainObject's data attribute.
    
        Here is an example of the other case, using the result of an
        elasticsearch query.
    
        instance = DomainObject(result['hits']['hits'][0])
        
        The data from the elasticsearch document's '_source' key would then
        be copied to instance.data .
        The rest (metadata) would be in instance.meta .
        '''
        if '_source' in kwargs:
            self.data = dict(kwargs['_source'])
            self.meta = dict(kwargs)
            del self.meta['_source']
        else:
            self.data = dict(kwargs)
            
    @classmethod
    def target(cls):
        return  cls.base_index_url + '/' + cls.__type__ + '/'

    @classmethod
    def initialise_index(cls):
        mappings = config.MAPPINGS
        for key, mapping in mappings.iteritems():
            im = cls.base_index_url + '/' + key + '/_mapping'
            exists = requests.get(im)
            if exists.status_code != 200:
                requests.post(cls.base_index_url) # create index
                requests.post(cls.base_index_url + '/' + key + '/test', data=json.dumps({'id':'test'})) # create type
                requests.delete(cls.base_index_url + '/' + key + '/' + 'test') # delete data used to create type
                r = requests.put(im, json.dumps(mapping))
                log.info(str(key) + ', ' + str(r.status_code))
                return r.status_code
            else:
                r = requests.put(im, json.dumps(mapping)) # update mapping
                return r.status_code
    
    @classmethod
    def makeid(cls):
        '''Create a new id for data object
        overwrite this in specific model types if required'''
        return uuid.uuid4().hex

    @property
    def id(self):
        return self.data.get('id', None)
        
    @property
    def version(self):
        return self.meta.get('_version', None)

    @property
    def json(self):
        return json.dumps(self.data)

    def save(self):
        if 'id' in self.data:
            id_ = self.data['id'].strip()
        else:
            id_ = self.makeid()
            self.data['id'] = id_
        
        self.data['last_updated'] = datetime.now().isoformat()

        if 'created_date' not in self.data:
            self.data['created_date'] = datetime.now().isoformat()
            
        if 'author' not in self.data:
            self.data['author'] = "anonymous"

        r = requests.post(self.target() + self.data['id'], data=json.dumps(self.data))

    @classmethod
    def bulk(cls, bibjson_list, idkey='id', refresh=False):
        data = ''
        for r in bibjson_list:
            data += json.dumps( {'index':{'_id':r[idkey]}} ) + '\n'
            data += json.dumps( r ) + '\n'
        r = requests.post(cls.target() + '_bulk', data=data)
        if refresh:
            cls.refresh()
        return r.json()


    @classmethod
    def refresh(cls):
        r = requests.post(cls.target() + '_refresh')
        return r.json()


    @classmethod
    def pull(cls, id_):
        '''Retrieve object by id.'''
        if id_ is None:
            return None
        try:
            out = requests.get(cls.target() + id_)
            if out.status_code == 404:
                return None
            else:
                return cls(**out.json())
        except:
            return None

    @classmethod
    def keys(cls,mapping=False,prefix=''):
        # return a sorted list of all the keys in the index
        if not mapping:
            mapping = cls.query(endpoint='_mapping')[cls.__type__]['properties']
        keys = []
        for item in mapping:
            if mapping[item].has_key('fields'):
                for item in mapping[item]['fields'].keys():
                    if item != 'exact' and not item.startswith('_'):
                        keys.append(prefix + item + config.FACET_FIELD)
            else:
                keys = keys + cls.keys(mapping=mapping[item]['properties'],prefix=prefix+item+'.')
        keys.sort()
        return keys
        
    @classmethod
    def query(cls, recid='', endpoint='_search', q='', terms=None, terms_operator='must', facets=None, **kwargs):
        '''
        Perform a query on backend.

        :param recid: needed if endpoint is about a record, e.g. mlt
        :param endpoint: default is _search, but could be _mapping, _mlt, _flt etc.
        :param q: maps to query_string parameter if string, or query dict if dict.
        :param terms: dictionary of terms to filter on. values should be lists. 
        :param terms_operator: "must" or "should"
        :param facets: dict of facets to return from the query.
        :param kwargs: any keyword args as per
            http://www.elasticsearch.org/guide/reference/api/search/uri-request.html
        '''
        if recid and not recid.endswith('/'): recid += '/'
        if isinstance(q,dict):
            query = q
        elif q:
            query = {'query': {'query_string': { 'query': q }}}
        else:
            query = {'query': {'match_all': {}}}

        if facets:
            if 'facets' not in query:
                query['facets'] = {}
            for k, v in facets.items():
                query['facets'][k] = {"terms":v}

        if terms:
            boolean = {terms_operator: [] }
            for term in terms:
                if not isinstance(terms[term],list): terms[term] = [terms[term]]
                for val in terms[term]:
                    obj = {'term': {}}
                    obj['term'][ term ] = val
                    boolean[terms_operator].append(obj)
            if q and not isinstance(q,dict):
                boolean[terms_operator].append( {'query_string': { 'query': q } } )
            elif q and 'query' in q:
                boolean[terms_operator].append( query['query'] )
            query['query'] = {'bool': boolean}

        for k,v in kwargs.items():
            if k == '_from':
                query['from'] = v
            else:
                query[k] = v

        if endpoint in ['_mapping']:
            r = requests.get(cls.target() + recid + endpoint)
        else:
            r = requests.post(cls.target() + recid + endpoint, data=json.dumps(query))

        try:
            return r.json()
        except JSONDecodeError:
            log.warn("elasticsearch query returned this non-JSON result:\n    " + r.text)
            raise

    def accessed(self):
        if 'last_access' not in self.data:
            self.data['last_access'] = []
        try:
            usr = current_user.id
        except:
            usr = "anonymous"
        self.data['last_access'].insert(0, { 'user':usr, 'date':datetime.now().strftime("%Y-%m-%d %H%M") } )
        r = requests.put(self.target() + self.data['id'], data=json.dumps(self.data))

    def delete(self):        
        r = requests.delete(self.target() + self.id)


