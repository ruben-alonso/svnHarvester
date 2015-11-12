'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''

DB_NAME = 'elastic_search_v1'

MULTI_PAGE = 'multi_page'

WEBSERVICES_INDEX_NAME = "webservices"
WEBSERVICES_DOCTYPE_NAME = "webservice"
WEBSERVICES_MAPPING = {
    "webservice": {
        "properties": {
            "name": {"type": "string"},
            "url": {"type": "string"},
            "query": {"type": "string"},
            "frequency": {"type": "string"},
            "active": {"type": "boolean"},
            "email": {"type": "string"},
            "end_date": {"type": "date"},
            "engine": {"type": "string"},
            "wait_window": {"type": "integer"}
        }
    }
}

TEMPORARY_INDEX_NAME = "temporary"
TEMPORARY_DOCTYPE_NAME = "document"
TEMPORARY_MAPPING = ""

HISTORY_INDEX_NAME = "history"
HISTORY_DOCTYPE_NAME = "historic"
HISTORY_MAPPING = {
    "historic": {
        "properties": {
            "date": {"type": "date"},
            "name_ws": {"type": "string"},
            "url": {"type": "string"},
            "query": {"type": "string"},
            "start_date": {"type": "date"},
            "end_date": {"type": "date"},
            "num_files_received": {"type": "integer"},
            "num_files_sent": {"type": "integer"},
            "error": {"type": "string"}
        }
    }
}

configMySQL = {
               'user': 'my',
               'password': 'sql',
               'host': '127.0.0.1',
               'database': 'articles',
               'raise_on_warnings': True,
               }

configES = [{'host': '52.30.3.136', 'port': 9200},
            {'host': 'localhost', 'port': 9200}]
