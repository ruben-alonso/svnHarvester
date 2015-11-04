'''
Created on 21 Oct 2015

Implementation of Importer for PubMed API 
More information about PubMed:
- http://www.ncbi.nlm.nih.gov/pubmed

@author: Mateusz.Kasiuba
'''
from root.nested.ImportAbstract import H_ImportAbstract

class H_ImportPubMed(H_ImportAbstract):

    _db = None
    _retmode = None
    _term = None

    def __init__(self):
        self._address = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
        self._db = 'pubmed'
        self._retmode = 'xml'

    def import_data(self):
        return H_ImportAbstract.import_data(self)
    
    def bulid_url(self):
        H_ImportAbstract.bulid_url(self)
        
    @property    
    def db(self):
        "check database witch we have to use to search records - default - pubmed"
        return self._db;
    
    @property    
    def term(self):
        "Entrez text query. All special characters must be URL encoded"
        return self._term;
    
    @db.setter
    def db(self,value):
        if self.is_valid_db(value):
            self._db = value
            
    @term.setter
    def term(self,value):
        if self.is_term(value):
            self._term = value
            
    def is_valid_db(self, name):
        'checking it is correct database selected from PubMed'
        if 'pubmed' == name:#now we accept only pubmed engine but there is a list of avaiable engines http://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo.fcgi
            self._db = name;
            
    def valid_term(self, variable):
        self._term = variable.replace(" ", "+")