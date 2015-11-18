import utils.connector.connector as DB
import utils.config as config
import json

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
        pass
    
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