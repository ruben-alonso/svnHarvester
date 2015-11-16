'''
Created on 03 Nov 2015
 
Main test class for EPMC
 
@author: Mateusz.Kasiuba
'''
from datetime import datetime
import unittest, os

from engine.query.QueryInvoker import H_QueryInvoker

from utils.config import MULTI_PAGE, DB_NAME
from utils.connector.connector import U_DBConnection


class TestQueryInvoker(unittest.TestCase):
      
    __test_file_path = os.path.dirname(os.path.abspath(__file__)) + "\\data_mock\\"
  
    def test_simple_import(self):
        DB = U_DBConnection().get_connection(DB_NAME)
        date = datetime.now().date()
        end_date = date.replace(month=11,day=20)
        start_date = date.replace(month=11,day=20)
        x = H_QueryInvoker().get_engine(MULTI_PAGE, DB,
        'http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&query=malaria%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D',
         start_date, end_date )
        
        number_of_result = x.execute()
        
        self.assertTrue(number_of_result >= 0) 
        
    def test_default_value(self):
        DB = U_DBConnection().get_connection(DB_NAME)
        x = H_QueryInvoker().get_engine(MULTI_PAGE, DB,
        'http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&query=malaria%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D')
        
        number_of_result = x.execute()
        
        self.assertTrue(number_of_result >= 0) 
        
    def test_change_dates_value(self):
        DB = U_DBConnection().get_connection(DB_NAME)
        x = H_QueryInvoker().get_engine(MULTI_PAGE, DB,
        'http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&query=malaria%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D')
        
        date = datetime.now().date()
        end_date = date.replace(month=11,day=20)
        start_date = date.replace(month=11,day=20)
        
        x.date_start = start_date
        x.date_end = end_date
        
        number_of_result = x.execute()
        
        self.assertTrue(number_of_result >= 0) 
        
    def test_date_setter_wrong(self):
        """ Checking date throwing exceptions """
        DB = U_DBConnection().get_connection(DB_NAME)
        x = H_QueryInvoker().get_engine(MULTI_PAGE, DB,
        'http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&query=malaria%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D')
          
        currdate = datetime.now();
        with self.assertRaises(TypeError):
            x.date_start = currdate
        with self.assertRaises(TypeError):
            x.date_end = currdate
        with self.assertRaises(TypeError):
            x.date_start = "2013-01-02"
        with self.assertRaises(TypeError):
            x.date_end = "2013-01-02"
        with self.assertRaises(TypeError):
            x.date_end = []
        with self.assertRaises(TypeError):
            x.date_start = []
            
if __name__ == '__main__':
    unittest.main()