'''
Created on 2 Nov 2015

@author: Ruben.Alonso
@Description: Package to run tests over the utils functions
'''
import unittest as UT
import config
import H_WS_Invoker as WI
import H_Exception_Handler as EH
import H_DB_Connector as DBC
from unittest.mock import patch


class WSInvokerTestCases(UT.TestCase):

    def test_xml_format_error(self):
        with self.assertRaises(EH.IncorrectFormatError):
            wsTest = WI.H_WSInvoker()
            wsTest.retrieve_information_xml("http://www.ebi.ac.uk/europepmc/webservices/rest/search/query=CREATION_DATE:%5B2014-09-23%20TO%202015-10-23%5D&resulttype=core&format=json")

    def test_json_format_error(self):
        with self.assertRaises(EH.IncorrectFormatError):
            wsTest = WI.H_WSInvoker()
            wsTest.retrieve_information_json("http://www.ebi.ac.uk/europepmc/webservices/rest/search/query=CREATION_DATE:%5B2014-09-23%20TO%202015-10-23%5D&resulttype=core&format=xml")

    def test_bad_url_xml(self):
        with self.assertRaises(EH.GenericError):
            wsTest = WI.H_WSInvoker()
            wsTest.retrieve_information_xml("a")

    def test_bad_url_json(self):
        with self.assertRaises(EH.GenericError):
            wsTest = WI.H_WSInvoker()
            wsTest.retrieve_information_json("b")


class DBConnectorTestCases(UT.TestCase):

    def test_connection_refused_port(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = [{'host': '10.100.13.46', 'port': 9201}]
            connObj = DBC.H_DBConnection().get_connection('elastic_search_v1')

    def test_connection_refused_addr(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = [{'host': '10.100.13.254', 'port': 9200}]
            connObj = DBC.H_DBConnection().get_connection('elastic_search_v1')

    def test_connection_accepted(self):
        config.configES = [{'host': '10.100.13.67', 'port': 9200}]
        connObj = DBC.H_DBConnection().get_connection('elastic_search_v1')
        self.assertIn("[{'host': '10.100.13.67', 'port': 9200}]",
                      str(connObj.connector), "whatever")

    def test_connection_empty_host(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = ""
            connObj = DBC.H_DBConnection().get_connection('elastic_search_v1')

if __name__ == '__main__':
    UT.main()
