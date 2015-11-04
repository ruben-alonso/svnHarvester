'''
Created on 21 Oct 2015

Abstract method of Importing system in Harvester, contain basic common variables and definictions of basics functions for an importer

@author: Mateusz.Kasiuba
'''
from abc import abstractmethod, ABCMeta
from flask._compat import with_metaclass
import urllib.request


class H_ImportAbstract(with_metaclass(ABCMeta)):

    _address = None
    _map = None
    _api_query = None
    
    def __init__(self):
        self._address = None
        self._map = {}
        self._api_query = ''

    @abstractmethod
    def import_data(self):
        self._bulid_url()
        try: 
            url = urllib.request.urlopen(self._api_query)
            if(200 != url.code):
                raise ValueError('Server respond code '+url.code)
            return url.read().decode('utf8')
        except urllib.error.URLError as e:
            raise e  
    
    @abstractmethod
    def _bulid_url(self):
        if(self._map is None):
            raise ValueError('Map cannot be empty!')
        address = self._address
        for name, variable in self._map.items():
            address += variable +'='+ str(eval('self.'+name))+'&'
        self._api_query = address[:-1]
    
#.########..########.########.####.##....##.########....##.....##....###....########..####....###....########..##.......########..######.
#.##.....##.##.......##........##..###...##.##..........##.....##...##.##...##.....##..##....##.##...##.....##.##.......##.......##....##
#.##.....##.##.......##........##..####..##.##..........##.....##..##...##..##.....##..##...##...##..##.....##.##.......##.......##......
#.##.....##.######...######....##..##.##.##.######......##.....##.##.....##.########...##..##.....##.########..##.......######....######.
#.##.....##.##.......##........##..##..####.##...........##...##..#########.##...##....##..#########.##.....##.##.......##.............##
#.##.....##.##.......##........##..##...###.##............##.##...##.....##.##....##...##..##.....##.##.....##.##.......##.......##....##
#.########..########.##.......####.##....##.########.......###....##.....##.##.....##.####.##.....##.########..########.########..######.
    
    @property    
    def _map(self):
        """Private property defines a map of field {"NameOfField":"Value"} which will be used in query """
    
    @_map.setter
    def _map(self,value):
        self._map = value

    @property
    def api_query(self):
        """ Contain last query called to API """
    
    @api_query.getter
    def api_query(self):
        return self._api_query
    
    
    
    
    
    
    