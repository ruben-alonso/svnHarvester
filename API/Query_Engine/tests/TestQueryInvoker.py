'''
Created on 03 Nov 2015
 
Main test class for EPMC
 
@author: Mateusz.Kasiuba
'''
# import unittest
# from QueryInvoker import H_QueryInvoker
# from datetime import date, datetime
# import mock, json, os, locale, sys
# import codecs, urllib
# import re
# from xml.etree.ElementTree import ElementTree, tostring, fromstring
# import mock
# from config import MULTI_PAGE
#  
# class TestImportEPMC(unittest.TestCase):
#      
#     __test_file_path = os.path.dirname(os.path.abspath(__file__)) + "\\data_mock\\"
#  
#     def test_invoker(self):
#         pass
#  
#     def test_simple_test_import_type_result(self):
#         multi_page = H_QueryInvoker(MULTI_PAGE)
#          
#         date = datetime.now().date()
#         end_date = date.replace(month=11,day=20)
#         start_date = date.replace(month=11,day=20)
#         x = H_QueryInvoker().get_engine(MULTI_PAGE,'http://www.ebi.ac.uk/europepmc/webservices/rest/\
#         search/resulttype=core&format=json&\
#         query=malaria%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D', start_date, end_date )
#         
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