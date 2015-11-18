import utils.connector.connector as DB

class HarvesterModel():
    '''
    Provides a querys for getting data from ES 
    '''
    def get_webservices(self):
        '''
        gets all webservices 
        
        Return:
            list of webbservices
        '''
        pass
    
    def get_historylist(self):
        '''
        gets all webservices 
        
        Return:
            list of historylist
        '''
        pass
    
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
        valid all information from webservice and save it if wverything goes ok
        
        '''
        pass