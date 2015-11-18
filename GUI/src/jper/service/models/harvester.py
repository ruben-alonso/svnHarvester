import utils.connector.connector as DB
import utils.config as config
import json
from werkzeug.datastructures import MultiDict

class HarvesterModel():
    '''
    Provides a querys for getting data from ES 
    '''
    
    def __init__(self):
        self.__conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        self.__match_all = {
            "query": {
                "match_all": {}
            }
        }
    
    def get_webservices(self):
        '''
        gets all webservices 
        
        Return:
            list of webbservices
        '''
        
        result = self.__conn.execute_search_query(config.WEBSERVICES_INDEX_NAME, config.WEBSERVICES_DOCTYPE_NAME, self.__match_all)
        return result['hits']['hits']
    
    def get_history(self):
        '''
        gets all webservices 
        
        Return:
            list of history
        '''
        return self.__conn.execute_search_query(config.HISTORY_INDEX_NAME, config.HISTORY_DOCTYPE_NAME, self.__match_all)
    
    def get_webservice(self, webservice_id):
        '''
        get webservice with all data
        
        Return:
            webservice object
        '''
        query = {
            "query": {
                "match": {
                    '_id' : str(webservice_id)
                              }
            }
        }
        
        print('query')
        
        result = self.__conn.execute_search_query(config.WEBSERVICES_INDEX_NAME, config.WEBSERVICES_DOCTYPE_NAME, query)
        return result['hits']['hits']
    
    def save_webservice(self, webservice):
        '''
        validate and save webservice object
        
        Return:
            webservice object
        '''
        pass
    
    def valid_webservice(self, webservice_id):
        ''' 
        valid all information from webservice and save it if everything goes ok
        
        '''
        pass
    
    def multidict_form_data(self,data):
        '''
        
        '''
        print(data)
        print(data[0]['_source'])
        return MultiDict(
                         [
                          ('url', data[0]['_source']['url']),
                          ('name',data[0]['_source']['name']),
                          ('query',data[0]['_source']['query']),
                          ('end_date',data[0]['_source']['end_date']),
                          ('frequency',data[0]['_source']['frequency']),
                          ('engine',data[0]['_source']['engine']),
                          ('active',data[0]['_source']['active']),
                          ('wait_window',data[0]['_source']['wait_window'])
                          ]
                         )