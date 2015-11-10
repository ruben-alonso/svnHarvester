# '''
# Created on 03 Nov 2015
# 
# Main test class for EPMC
# 
# @author: Mateusz.Kasiuba
# '''
# import unittest
# from ImportEPMC import H_ImportEPMC
# from datetime import date, datetime
# import mock, json, os, locale, sys
# import codecs, urllib
# import re
# from xml.etree.ElementTree import ElementTree, tostring, fromstring
# import mock
# 
# class TestImportEPMC(unittest.TestCase):
#     
#     __test_file_path = os.path.dirname(os.path.abspath(__file__)) + "\\data_mock\\"
# 
#     def test_date_setter(self):
#         """Checking date setter - succes sceonario"""
#         epmc = H_ImportEPMC()
#         
#         #Checking default setting should be from first previous month to first this month
#         currdate = date.today()
#         self.assertTrue(epmc.end_date == currdate.replace(day=1))
#         last_month = 12 if 0 == currdate.month-1 else currdate.month-1
#         year = currdate.year
#         if(12==last_month):
#             year = currdate.year-1
#         self.assertTrue(epmc.start_date == currdate.replace(month=last_month,day=1,year=year))
#         
#         #Checking setter and getter
#         epmc.end_date = currdate.replace(month=10,day=28)
#         epmc.start_date = currdate.replace(month=10,day=20,year=2000)        
#         self.assertTrue(epmc.end_date == currdate.replace(month=10,day=28))
#         self.assertTrue(epmc.start_date == currdate.replace(month=10,day=20,year=2000))
# 
#     def test_new_year_import(self):
#         """
#         TODO - I dont know how to mock date.today() function to 
#         return new year date in QueryEngineMultiPage and
#         
#         http://www.toptal.com/python/an-introduction-to-mocking-in-python
#         
#         test correctly default setters more:
#         https://docs.python.org/3/library/unittest.mock-examples.html#partial-mocking 
#         """
#         pass
# 
#         
#     def test_date_setter_wrong(self):
#         """ Checking date throwing exceptions """
#         epmc = H_ImportEPMC()
#         
#         currdate = datetime.now();
#         with self.assertRaises(TypeError):
#             epmc.start_date = currdate
#         with self.assertRaises(TypeError):
#             epmc.end_date = currdate
#         with self.assertRaises(TypeError):
#             epmc.start_date = "2013-01-02"
#         with self.assertRaises(TypeError):
#             epmc.end_date = "2013-01-02"
#         with self.assertRaises(TypeError):
#             epmc.end_date = []
#         with self.assertRaises(TypeError):
#             epmc.start_date = []
#             
#     def test_format_setter(self):
#         epmc = H_ImportEPMC()
#         
#         self.assertTrue('json', epmc.format)#default value
#         epmc.format = 'XML'
#         self.assertTrue('XML', epmc.format)
#         epmc.format = 'DC'
#         self.assertTrue('DC', epmc.format)
#         epmc.format = 'json'
#         self.assertTrue('json', epmc.format)
#         
#     def test_format_setter_wrong(self):
#         epmc = H_ImportEPMC()
#         
#         with self.assertRaises(TypeError):
#             epmc.format = 'html'
#     
#     def test_resulttype_setter(self):
#         epmc = H_ImportEPMC()
#         
#         self.assertTrue('core', epmc.resulttype)#default value
#         epmc.resulttype = 'idlist'
#         self.assertTrue('idlist', epmc.resulttype)
#         epmc.resulttype = 'lite'
#         self.assertTrue('lite', epmc.resulttype)
#         epmc.resulttype = 'core'
#         self.assertTrue('core', epmc.resulttype)
#         
#     def test_resulttype_setter_wrong(self):
#         epmc = H_ImportEPMC()
#         
#         with self.assertRaises(TypeError):
#             epmc.resulttype = 'wrong_provider'
#         
#     def test_ext_id_setter(self):
#         epmc = H_ImportEPMC()
#         
#         epmc.ext_id = '1234'
#         self.assertTrue(epmc.ext_id == '1234')
#         epmc.ext_id = '4321'
#         self.assertTrue(epmc.ext_id == '4321')
#         epmc.ext_id = ''
#         self.assertTrue(epmc.ext_id == '')
#         
#     def test_page_setter(self):
#         epmc = H_ImportEPMC()
#         
#         self.assertTrue(1 == epmc.page)
#         epmc.page = 2
#         self.assertTrue(2 == epmc.page)
#         
#     def test_page_setter_wrong(self):
#         epmc = H_ImportEPMC()
#         
#         with self.assertRaises(ValueError):
#             epmc.page = 'e'
#     
#     def test_page_size_setter(self):
#         epmc = H_ImportEPMC()
#         
#         self.assertTrue(epmc.page_size == 100)
#         epmc.page_size = 120
#         self.assertTrue(epmc.page_size == 120)
#         
#     def test_query_setter(self):
#         epmc = H_ImportEPMC()
#         
#         epmc.query = 'malaria'
#         self.assertTrue(epmc.query == 'malaria')
#     
#     def test_page_size_setter_wrong(self):
#         epmc = H_ImportEPMC()
#         
#         with self.assertRaises(ValueError):
#             epmc.page_size = 1001
#         with self.assertRaises(ValueError):
#             epmc.page_size = 0
#         with self.assertRaises(TypeError):
#             epmc.page_size = 'e'
#             
#     def test_simple_test_import_type_result(self):
#         epmc = H_ImportEPMC()
#         
#         epmc.page_size = 50
#         epmc.query = 'malaria'
#         epmc.start_date = date(2015,10,2)
#         epmc.end_date = date(2015,10,2)
#         
#         data = epmc.import_all()    
#         self.assertTrue(type(data) is dict)
#         
#     def test_unicode_encode_data(self):
#         epmc = H_ImportEPMC()
#         epmc.start_date = date(2015,2,3)
#         epmc.end_date = date(2015,2,3)
#         
#         epmc.page_size = 50
#         epmc.query = 'malaria'
#         
#         data = epmc.import_all()    
# #         print(data['resultList']['result'])
# #         locale.setlocale(category, locale)
#         #TODO printing data is impossible because windows console cannot use utf8 - check how to change this
#         
#     def test_import_by_query_and_date_and_ext_id(self):
#         epmc = self.__init_object__()
#         data = epmc.import_data()
#          
#         read_data = self.__load_file__('correctjson.json');
#          
#         correct_data = json.loads(read_data)
#          
#         #remove luceneScore is dynamic created by epmc
#         del data['resultList']['result'][0]['luceneScore']
#          
#         self.assertTrue(data == correct_data)
#         
#     def test_wrong_address_provider(self):
#         epmc = H_ImportEPMC()
#         
#         with self.assertRaises(ValueError):
#             epmc._address = 'test'
#             epmc.import_data()
#          
#         with self.assertRaises(urllib.error.HTTPError):#collect 404, 500 etc
#             epmc._address = 'http://google.co.uk/'
#             epmc.import_data()
#     
#     def test_wrong_amount_in_import_all_data(self):
#         """
#         TODO FINISH!
#         Nedd to use a mock who will replace def QueryEngineMultiPage.__get_number_of_pages with wrong number of pages and catch ValueError
#         
#         http://www.toptal.com/python/an-introduction-to-mocking-in-python
#         """
#         pass
#     
#     def test_import_data_as_xml(self):
#         epmc = self.__init_object__()
#         epmc.format = 'XML'
#         data = epmc.import_data()
#          
#         read_data = self.__load_file__('correctxml.xml');
#          
#         read_data = fromstring(read_data) 
#          
#         xmlstr = tostring(data, encoding='utf8', method='xml')
#         correctstr = tostring(read_data, encoding='utf8', method='xml')
#          
#         #Lucene Score is dynamic generated need to remove it before compare
#         xmlstr = re.sub('<luceneScore>.*</luceneScore>', '', xmlstr.decode('utf-8'))
#          
#         self.assertTrue(xmlstr == correctstr.decode('utf-8'))
#      
#     def test_import_data_as_dc(self):
#         epmc = self.__init_object__()
#         epmc.format = 'DC'
#         data = epmc.import_data()
#         
#         read_data = self.__load_file__('correctdc.xml');
#         
#         read_data = fromstring(read_data) 
#         
#         xmlstr = tostring(data, encoding='utf8', method='xml')
#         correctstr = tostring(read_data, encoding='utf8', method='xml')
#         
#         self.assertTrue(xmlstr == correctstr)
#     
#     def test_import_resulttype_as_idlist(self):
#         epmc = self.__init_object__()
#         epmc.resulttype = 'idlist'
#         data = epmc.import_data()
#         
#         read_data = self.__load_file__('correctjsonidlist.json')
#                 
#         correct_data = json.loads(read_data)
#         
#         self.assertTrue(data == correct_data)
#         
#     def test_import_resulttype_as_lite(self):
#         epmc = self.__init_object__()
#         epmc.resulttype = 'lite'
#         data = epmc.import_data()
#          
#         read_data = self.__load_file__('correctjsonlite.json')
#         
#         #remove luceneScore is dynamic created by epmc
#         del data['resultList']['result'][0]['luceneScore']
#          
#         correct_data = json.loads(read_data)
#          
#         self.assertTrue(data == correct_data)
#         
#     def __load_file__(self,filename):
#         read_data = '';
#         with open(self.__test_file_path+filename, 'r') as f:
#             read_data = f.read()
#         f.closed
#         
#         return read_data
#         
#     def __init_object__(self):
#         epmc = H_ImportEPMC()
#         
#         epmc.query = "responsible"
#         epmc.page_size = 10
#         date = datetime.now().date()
#         epmc.end_date = date.replace(month=10,day=28)
#         epmc.start_date = date.replace(month=10,day=20,year=2000)
#         epmc.resulttype = 'core'
#         epmc.page = '1'
#         epmc.ext_id = 26330629
#         
#         return epmc
#     
# if __name__ == '__main__':
#     unittest.main()