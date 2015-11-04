'''
Created on 2 Nov 2015

@author: Ruben.Alonso
@Description: Package to run tests over the utils functions
'''
import unittest as UT
from H_Utils_Unit_Tests import configTest
import H_WS_Invoker as WI
import H_Exception_Handler as EH
import H_DB_Connector as DBC
import config

# class WSInvokerTestCases(UT.TestCase):
# 
#     def test_xml_format_error(self):
#         with self.assertRaises(EH.IncorrectFormatError):
#             wsTest = WI.H_WSInvoker()
#             wsTest.retrieve_information_xml(configTest.jsonURLQuery)
# 
#     def test_json_format_error(self):
#         with self.assertRaises(EH.IncorrectFormatError):
#             wsTest = WI.H_WSInvoker()
#             wsTest.retrieve_information_json(configTest.xmlURLQuery)
# 
#     def test_bad_url_xml(self):
#         with self.assertRaises(EH.GenericError):
#             wsTest = WI.H_WSInvoker()
#             wsTest.retrieve_information_xml(configTest.badURLQuery)
# 
#     def test_bad_url_json(self):
#         with self.assertRaises(EH.GenericError):
#             wsTest = WI.H_WSInvoker()
#             wsTest.retrieve_information_json(configTest.badURLQuery)
# 
#     def test_json_expected_output(self):
#         wsTest = WI.H_WSInvoker()
#         result = wsTest.retrieve_information_json(configTest.jsonURLQuery)
#         self.assertEqual(str(result),
#                          configTest.jsonResult,
#                          "Great")
# 
#     def test_xml_expected_output(self):
#         wsTest = WI.H_WSInvoker()
#         result = wsTest.retrieve_information_xml(configTest.xmlURLQuery)
#         self.assertEqual(result, configTest.xmlResult, "Great")


class DBConnectorTestCases(UT.TestCase):

#     def test_connection_refused_port(self):
#         with self.assertRaises(EH.DBConnectionError):
#             config.configES = configTest.invalidPortESConn
#             connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
#             connObj.close()
# 
#     def test_connection_refused_bad_DB_name(self):
#         with self.assertRaises(EH.InputError):
#             config.configES = configTest.validESConn
#             connObj = DBC.H_DBConnection().get_connection(configTest.invalidESName)
#             connObj.close()
# 
#     def test_connection_closed(self):
#         config.configES = configTest.validESConn
#         connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
#         connObj.close()
#         DBC.H_DBConnection().del_connection()
#         self.assertIs(DBC.H_DBConnection().__conn, None, "Ok")
# 
#     def test_connection_refused_addr(self):
#         with self.assertRaises(EH.DBConnectionError):
#             config.configES = configTest.invalidAddrESConn
#             connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
#             connObj.close()
# 
#     def test_connection_accepted(self):
#         config.configES = configTest.validESConn
#         connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
#         try:
#             self.assertIn(configTest.stringESConn,
#                           str(connObj.connector),
#                           "Connected")
#         except AssertionError:
#             self.assertIn(configTest.stringESConnSwap,
#                           str(connObj.connector),
#                           "Connected")
#         finally:
#             connObj.close()
# 
#     def test_connection_empty_host(self):
#         with self.assertRaises(EH.DBConnectionError):
#             config.configES = configTest.emptyConn
#             connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
#             connObj.close()

    def test_insert_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                connObj.execute_insert_query("one", "doc", "badExample")
            finally:
                connObj.close()

    def test_insert_correct_format(self):
        jsonVariable = {"a": 0, "b": 0, "c": 0}
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable)
#         print(result)
        self.assertTrue(result['created'], "Ok")
        connObj.close()

    def test_search_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                connObj.execute_search_query("one", "doc", "badExample")
            finally:
                connObj.close()

    def test_search_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_search_query(index='test',
                                              docType='unittest',
                                              body={"query": {"match_all": {}}}
                                              )
#         print(result)
        self.assertIsNotNone(result, "Good")
        connObj.close()

    def test_create_table_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                result = connObj.execute_create_table("BADTABLE")
                print(result)
            finally:
                connObj.close()

    def test_create_table_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_create_table(index='goodtable')
        self.assertIsNotNone(result, "Good")
        connObj.close()

    def test_create_doctype_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                try:
                    mapping = {
                        "trip": {
                            "properties": {
                                "duration": {"type": "integer"},
                                "start_date": {"type": "string"},
                                "baaaaad":[{[{}]}]
                            }
                        }
                    }
                    result = connObj.execute_create_doc_type(index="test",
                                                         docType="test_doc",
                                                         mapping=mapping)
                    print(str(result))
                except TypeError as err:
                    print(str(err))
            finally:
                connObj.close()

    def test_create_doctype_incorrect_index(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.InputError):
            try:
                try:
                    mapping = {
                        "trip": {
                            "properties": {
                                "duration": {"type": "integer"},
                                "start_date": {"type": "string"},
                                "baaaaad":[{[{}]}]
                            }
                        }
                    }
                    result = connObj.execute_create_doc_type(index="index",
                                                         docType="test_doc",
                                                         mapping=mapping)
                    print(str(result))
                except TypeError as err:
                    print(str(err))
            finally:
                connObj.close()

    def test_create_doctype_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.H_DBConnection().get_connection(configTest.validESName)
        mapping = {
            "trip": {
                "properties": {
                    "duration": {"type": "integer"},
                    "start_date": {"type": "string"}
                }
            }
        }
        result = connObj.execute_create_doc_type(index="test",
                                                 docType="test_doc",
                                                 mapping=mapping)
        print(result)
        self.assertTrue(result['created'], "Good")
        connObj.close()

if __name__ == '__main__':
    UT.main()
