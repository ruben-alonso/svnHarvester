'''
Created on 06 Nov 2015

Engine invoker

@author: Mateusz.Kasiuba
'''
from Query_Engine.QueryEngineMultiPage import H_QueryEngineMultiPage
from config import MULTI_PAGE


class H_QueryInvoker():
    
    @staticmethod
    def get_engine(engine_name, db, url, date_start = 0, date_end = 0):
        if(engine_name == MULTI_PAGE):
            return H_QueryEngineMultiPage(db, url, date_start, date_end)
        
        raise ValueError('Engine %s do not exists!'% engine_name)