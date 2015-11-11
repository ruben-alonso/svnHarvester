'''
Created on 21 Oct 2015

Engine for EPMC multi page query 
To get more information about fields:
http://europepmc.org/Help#searchbyID
Written for "version":"4.3" API

Using config static:
DB_NAME
TEMPORARY_INDEX_NAME
TEMPORARY_DOCTYPE_NAME

@author: Mateusz.Kasiuba
'''
from Query_Engine.QueryEngine import H_QueryEngine

from datetime import date, timedelta
from OA_WS_Invoker.WS_Invoker import H_WSInvoker
import OA_Logging_Handler.Logging_Handler as LH
from config import TEMPORARY_DOCTYPE_NAME, TEMPORARY_INDEX_NAME


class H_QueryEngineMultiPage(H_QueryEngine):
    
    def __init__(self,DB, url,date_start = 0,date_end = 0):
        """
        By default start date is 01-Last Month-Current Year end date is 01-Current Month-Current Year, should be date object
        
        Values:
            self - string url with {start_date} as a start date and {end_date} as a end date
            date_start - date obj
            date_end - date obj
        
        """
        LH.fileLogger.info("Creating new MultiPage object params: %s %s %s" % (url,str(date_start),str(date_end)))
        if(0 == date_start):
            prevdate = date.today() - timedelta(days=2)
            date_start = prevdate.replace(day=1)
        if(0 == date_end):
            date_end = date.today().replace(day=1)
        
        self.__current_page = 1
        self.__api_query = ''
        self._date_start = date_start
        self._date_end = date_end
        self._db = DB
        self._url = url.format(
                               start_date = self._date_start.isoformat(), 
                               end_date = self._date_end.isoformat()
                               )
        
    def execute(self):
        """
        Import all data from EPMC using setted variables
        
        Params:
            DB - Object of H_DBConnection
        
        Returns:
            Boolean
        """
        invoker = H_WSInvoker()
        while True:
            self._bulid_query()
            LH.fileLogger.info("Execute: %s" % self.__api_query)
            data = invoker.retrieve_information_json(self.__api_query)
            if(0 == len(data['resultList']['result'])):
                break
            for item in data['resultList']['result']:
                self._db.execute_insert_query(TEMPORARY_INDEX_NAME, TEMPORARY_DOCTYPE_NAME, item)
            self.__current_page += 1
        return data['hitCount']
        
    def _bulid_query(self):
        """Bulid default url and put the start and end date - without page and page number"""
        self.__api_query = self._url + '&pageSize=1000&page=%s' % str(self.__current_page)


#.##.....##....###....##.......####....###....########..########.########
#.##.....##...##.##...##........##....##.##...##.....##....##....##......
#.##.....##..##...##..##........##...##...##..##.....##....##....##......
#.##.....##.##.....##.##........##..##.....##.##.....##....##....######..
#..##...##..#########.##........##..#########.##.....##....##....##......
#...##.##...##.....##.##........##..##.....##.##.....##....##....##......
#....###....##.....##.########.####.##.....##.########.....##....########

    def __valide_date(self,variable):
        """
        Private method - validate date
        
        Args:
            value - date
        
        Returns:
            Boolean
        """
        if type(variable) is date:
            return True
        return False
    
#.########..########.########.####.##....##.########....##.....##....###....########..####....###....########..##.......########..######.
#.##.....##.##.......##........##..###...##.##..........##.....##...##.##...##.....##..##....##.##...##.....##.##.......##.......##....##
#.##.....##.##.......##........##..####..##.##..........##.....##..##...##..##.....##..##...##...##..##.....##.##.......##.......##......
#.##.....##.######...######....##..##.##.##.######......##.....##.##.....##.########...##..##.....##.########..##.......######....######.
#.##.....##.##.......##........##..##..####.##...........##...##..#########.##...##....##..#########.##.....##.##.......##.............##
#.##.....##.##.......##........##..##...###.##............##.##...##.....##.##....##...##..##.....##.##.....##.##.......##.......##....##
#.########..########.##.......####.##....##.########.......###....##.....##.##.....##.####.##.....##.########..########.########..######.

    @property
    def date_start(self):
        """Set start date for searching publications by date of entry into the \
        Europe PMC database, in YYYY-MM-DD format; note syntax for searching \
        date range. By default last month"""
        return self._date_start
        
    @date_start.setter
    def date_start(self,value):
        if(self.__valide_date(value)):
            self._date_start = value
        else:
            raise TypeError('Date must be a datetime.date, not a %s' % type(value))
        
    @property
    def date_end(self):
        """Set end date for searching publications by date of entry into the \
        Europe PMC database, in YYYY-MM-DD format; note syntax for searching \
        date range. By default last month"""  
        return self._date_end
    
    @date_end.setter
    def date_end(self,value):
        if(self.__valide_date(value)):
            self._date_end = value
        else:
            raise TypeError('Date must be a datetime.date, not a %s' % type(value))
    
    @date_end.getter
    def date_end(self):
        return self._date_end
    
    @property
    def db(self):
        """Set end date for searching publications by date of entry into the \
        Europe PMC database, in YYYY-MM-DD format; note syntax for searching \
        date range. By default last month"""  
        return self._date_end
    
    @date_end.setter
    def date_end(self,value):
        if(self.__valide_date(value)):
            self._date_end = value
        else:
            raise TypeError('Date must be a datetime.date, not a %s' % type(value))
    
    @date_end.getter
    def date_end(self):
        return self._date_end
    
