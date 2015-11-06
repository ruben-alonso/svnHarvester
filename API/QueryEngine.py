'''
Created on 21 Oct 2015

Abstract class of engine
Engine control how to execute url to API-s

@author: Mateusz.Kasiuba
'''
from abc import abstractmethod, ABCMeta
from flask._compat import with_metaclass


class H_QueryEngine(with_metaclass(ABCMeta)):

    @abstractmethod
    def execute(self):
        pass
     
    @abstractmethod
    def _bulid_query(self):
        pass
