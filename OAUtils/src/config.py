'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''

DB_NAME = 'elastic_search_v1'

WEBSERVICES_INDEX_NAME = "webservices"
WEBSERVICES_DOCTYPE_NAME = "webservice"

TEMPORARY_INDEX_NAME = "temporary"
TEMPORARY_DOCTYPE_NAME = "document"

HISTORY_INDEX_NAME = "history"
HISTORY_DOCTYPE_NAME = "historic"

configMySQL = {
               'user': 'my',
               'password': 'sql',
               'host': '127.0.0.1',
               'database': 'articles',
               'raise_on_warnings': True,
               }

configES = [{'host': '10.100.13.103', 'port': 9200},
            {'host': 'localhost', 'port': 9200}]
