'''
Created on 26 Oct 2015

@author: Ruben.Alonso
@modified_by: Mateusz Kasiuba
'''

DB_NAME = "elastic_search_v1"

TEMPORARY_INDEX_NAME = "temporary"
WEBSERVICES_INDEX_NAME = "webServices"
HISTORY_INDEX_NAME = "history"

TEMPORARY_DOCTYPE_NAME = "document"
WEBSERVICES_DOCTYPE_NAME = "webservice"
HISTORY_DOCTYPE_NAME = "historic"

#Engine names
MULTI_PAGE = 'multi_page'

configMySQL = {
               'user': 'my',
               'password': 'sql',
               'host': '127.0.0.1',
               'database': 'articles',
               'raise_on_warnings': True,
               }

configES = [{'host': '10.100.13.51', 'port': 9200}]
