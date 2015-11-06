'''
Created on 06 Nov 2015

Engine invoker

@author: Mateusz.Kasiuba
'''
from QueryEngineMultiPage import H_QueryEngineMultiPage
from config import MULTI_PAGE


class H_QueryInvoker():

    def get_engine(self,engine_name, url, date_start = 0, date_end = 0):
        if(engine_name == MULTI_PAGE):
            return H_QueryEngineMultiPage(url, date_start, date_end)
        
        raise ValueError('Engine %s do not exists!'% engine_name)