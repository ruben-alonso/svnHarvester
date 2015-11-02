'''
Created on 21 Oct 2015

Class created for EPMC import
To get more information about fields:
http://europepmc.org/Help#searchbyID
Writed for "version":"4.3" API


@author: Mateusz.Kasiuba
'''
from root.nested.ImportAbstract import H_ImportAbstract

import json
import math
import urllib
from datetime import date


class H_ImportEPMC(H_ImportAbstract):
    
    _resulttype = None
    _query = None
    _ext_id = None
    _format = None
    _start_date = None
    _end_date = None
    _page = None
    _page_size = None
    _map = None
    _epmc_query = None
    _address = None 
    _current_page = None
    
    
    def __init__(self):
        self._address = 'http://www.ebi.ac.uk/europepmc/webservices/rest/search/'
        self._resulttype = 'core'
        self._format = 'json'
        currdate = date.today()
        last_month = 12 if 0 == currdate.month-1 else currdate.month-1
        year = currdate.year
        if(12==last_month):
            year = currdate.year-1
        self._start_date = currdate.replace(month=last_month,day=1,year=year)
        self._end_date = date.today()
        self._page = 1
        self._page_size = 100
        self._map = {'_epmc_query':'query', '_format':'format',
                     '_current_page':'page', '_page_size':'pageSize', 
                     '_resulttype' : 'resulttype'}

    def __get_number_of_pages(self):
        """
        Private Method - return number of pages for our query
        Retruns:
            int
        """
        tmp = self._page_size
        self._page_size = 1
        data = self.import_data(self._page,1)
        self._page_size = tmp
        return math.ceil(data['hitCount']/self._page_size)
        
    def import_all(self):
        """
        Import all data from EPMC using setted variables
        
        Returns:
            JSON
            XML
            DC
        """
        data = {'resultList': {'result': []}} #create a EPMC JSON structure
        pages = self.__get_number_of_pages()
        for page in range(0,pages):
            tmp = self.import_data(page+1)
            data['resultList']['result'] += tmp['resultList']['result']; 
        self.__validate_number_of_result(len(data['resultList']['result']))
        return data

    def __validate_number_of_result(self,number):
        """
        Private method 
        
        Throws: 
            ValueError
        
        Returns:
            Boolean
        """
        tmp = self._page_size
        self._page_size = 1
        data = self.import_data(self._page,1)
        self._page_size = tmp
        if(number == data['hitCount']):
            return True
        raise ValueError('Imported records is not equal API hit Count \
            Imported records: %s Hit Count: %s' 
            % str(number), str(len(data['resultList']['result'])))

    def import_data(self,page = None,page_size = None):
        """
        Import part of data from EPMC specified by page and page size
        
        Args:
            page: int
            page_size: int
        
        Throws: 
            ValueError
        
        Returns:
            JSON
            XML
            DC
        """
        page = page if page is not None else self._page
        page_size = page_size if page_size is not None else self._page_size
        self._current_page = page
        data = json.loads(H_ImportAbstract.import_data(self))
        if('errCode' in data.keys()) and (404 == data["errCode"]):
            raise ValueError('API respond code %s With Message %s' % (str(data['errCode']), data['errMsg']))
        return data
            
    def _bulid_url(self):
        """
        Private method - bulid a query url
        
        Returns:
            None
        """
        self._epmc_query = self._query + urllib.request.quote(" CREATION_DATE:[%s TO %s]" % 
                                                              (self.__get_date(self._start_date), 
                                                              self.__get_date(self._end_date)))
        if(self._ext_id is not None):
            self._epmc_query += urllib.request.quote(' AND EXT_ID:%s' % self.ext_id)
        H_ImportAbstract._bulid_url(self)

    def __get_date(self,date):
        """
        Private method - bulid a string from date object
        
        Args:
            date: date
        
        Returns:
            String (YYYY-MM-DD)
        """
        return str(date.year)+"-"+str('{:02d}'.format(date.month))+"-"+str('{:02d}'.format(date.day))
        
#.##.....##....###....##.......####....###....########..########.########
#.##.....##...##.##...##........##....##.##...##.....##....##....##......
#.##.....##..##...##..##........##...##...##..##.....##....##....##......
#.##.....##.##.....##.##........##..##.....##.##.....##....##....######..
#..##...##..#########.##........##..#########.##.....##....##....##......
#...##.##...##.....##.##........##..##.....##.##.....##....##....##......
#....###....##.....##.########.####.##.....##.########.....##....########
    
    def __valid_page_size(self, variable):
        """
        Private method - validate page size
        
        Args:
            varlue - int
        
        Returns:
            Boolean
        """
        if(isinstance( variable, int ) & (1 <= variable <= 1000)):
            return True
        return False
        
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
    
    def __validate_resulttype(self,value):
        """
        Private method - validate engine of resulttype
        
        Args:
            variable - str
        
        Returns:
            Boolean
        """
        validate_core = {'idlist', 'core', 'lite'}
        if value in validate_core:
            return True
        return False
    
    def __validate_format(self,value):
        """
        Private method - validate return format
        
        Args:
            variable - str
        
        Returns:
            Boolean
        """
        validate_core = {'DC', 'json', 'XML'}
        if value in validate_core:
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
    def page_size(self):
        """Define page size of import packages a value should be int and range is between 1 and 1000"""
        return self._page_size
    @page_size.setter
    def page_size(self,value):
        if(self.__valid_page_size(value)):
            self._page_size = value
        else:
            raise ValueError('Page size must be int between 1 and 1000 not a %s' % type(value))
    
    @property    
    def query(self):
        """Main query in EPMC API"""
        return self._query;
    
    @query.setter
    def query(self,value):
        self._query = str(value)
    
    @property
    def start_date(self):
        """Set start date for searching publications by date of entry into the \
        Europe PMC database, in YYYY-MM-DD format; note syntax for searching \
        date range. By default last month"""
        return self._start_date
        
    @start_date.setter
    def start_date(self,value):
        if(self.__valide_date(value)):
            self._start_date = value
        else:
            raise TypeError('Date must be a datetime.date, not a %s' % type(value))
        
    @property
    def end_date(self):
        """Set end date for searching publications by date of entry into the \
        Europe PMC database, in YYYY-MM-DD format; note syntax for searching \
        date range. By default last month"""  
        return self._end_date
    
    @end_date.setter
    def end_date(self,value):
        if(self.__valide_date(value)):
            self._end_date = value
        else:
            raise TypeError('Date must be a datetime.date, not a %s' % type(value))
    
    @property
    def resulttype(self):
        """The result type can either be idlist, core or lite (default - core):\
        idlist returns a list of IDs and sources for the given search terms\
        lite returns key metadata for the given search terms; this is the default value if the parameter is unspecified.\
        core returns full metadata for a given publication ID; including abstract, full text links, and MeSH terms."""
        return self._resulttype
    
    @resulttype.setter
    def resulttype(self,value):
        if(self.__validate_resulttype(value)):
            self._resulttype = value
        else:
            raise TypeError('Result type must be idlist, lite or core not a %s' % value)
    
    @property
    def page(self):
        """Define a page from where we should import data"""
        return self._page
    
    @page.setter
    def page(self, value):
        if(int(value)):
            self._page = value
   
    @property
    def ext_id(self):
        """Search for a publication by external ID: i.e. the ID assigned to a\
        publication at repository level. Together with the publication's \
        source, they form a unique id of the publication. Here is more details:\
        http://europepmc.org/Help#searchbyID"""
        return self._ext_id;
    @ext_id.setter
    def ext_id(self, value):
        self._ext_id = str(value)
    
    @property
    def format(self):
        """The format can either be XML, JSON(default) or DC (Dublin Core); the default \
        value is XML if the parameter is unspecified. XML returns the same\
        response as the SOAP web service; see the SOAP Web Service Reference Guide. \
        Note that the resulttype parameter associated with the DC response is always set to core.'
        return self._format"""
    
    @format.setter
    def format(self,value):
        if(self.__validate_format(value)):
            self._format = value
        else:
            raise TypeError('Result type must be JSON, DC or XML not a %s' % value)
   
   
   
    