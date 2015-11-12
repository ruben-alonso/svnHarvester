'''
Created on 2 Nov 2015

@author: Ruben.Alonso
@Description: Package to run tests over the utils functions
'''
import unittest as UT
import configTest
import utils.invoker.invoker as WI
import utils.exception.handler as EH
import utils.connector.connector as DBC
import config


class U_WSInvokerTestCases(UT.TestCase):

    def test_xml_format_error(self):
        with self.assertRaises(EH.IncorrectFormatError):
            wsTest = WI.U_WSInvoker()
            wsTest.retrieve_information_xml(configTest.jsonURLQuery)

    def test_json_format_error(self):
        with self.assertRaises(EH.IncorrectFormatError):
            wsTest = WI.U_WSInvoker()
            wsTest.retrieve_information_json(configTest.xmlURLQuery)

    def test_bad_url_xml(self):
        with self.assertRaises(EH.GenericError):
            wsTest = WI.U_WSInvoker()
            wsTest.retrieve_information_xml(configTest.badURLQuery)

    def test_bad_url_json(self):
        with self.assertRaises(EH.GenericError):
            wsTest = WI.U_WSInvoker()
            wsTest.retrieve_information_json(configTest.badURLQuery)

    def test_json_expected_output(self):
        wsTest = WI.U_WSInvoker()
        result = wsTest.retrieve_information_json(configTest.jsonURLQuery)
        self.maxDiff = None
        self.assertEqual(str(result),
                         configTest.jsonResult,
                         "Great")

    def test_xml_expected_output(self):
        wsTest = WI.U_WSInvoker()
        result = wsTest.retrieve_information_xml(configTest.xmlURLQuery)
        self.maxDiff = None
        self.assertEqual(result, configTest.xmlResult, "Great")


class U_DBConnectorTestCases(UT.TestCase):

    def test_connection_refused_port(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = configTest.invalidPortESConn
            connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
            connObj.close()

    def test_connection_refused_bad_DB_name(self):
        with self.assertRaises(EH.InputError):
            config.configES = configTest.validESConn
            connObj = DBC.U_DBConnection().get_connection(configTest.invalidESName)
            connObj.close()

    def test_connection_closed(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        connObj.close()
        DBC.U_DBConnection().del_connection()
        self.assertIs(DBC.U_DBConnection().__conn, None)

    def test_connection_refused_addr(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = configTest.invalidAddrESConn
            connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
            connObj.close()

    def test_connection_accepted(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        try:
            self.assertIn(configTest.stringESConn,
                          str(connObj.connector))
        except AssertionError:
            self.assertIn(configTest.stringESConnSwap,
                          str(connObj.connector))
        finally:
            connObj.close()

    def test_connection_empty_host(self):
        with self.assertRaises(EH.DBConnectionError):
            config.configES = configTest.emptyConn
            connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
            connObj.close()

    def test_connection_again_different_DB(self):
        config.configES = configTest.validESConn
        connObj1 = DBC.U_DBConnection().get_connection(configTest.validESName)
        connObj2 = DBC.U_DBConnection().get_connection(configTest.invalidESName)
        try:
            self.assertEqual(connObj1, connObj2)
        finally:
            connObj1.close()

    def test_insert_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                connObj.execute_insert_query("one", "doc", "badExample")
            finally:
                connObj.close()

    def test_insert_doc_correct_format(self):
        jsonVariable = {"a": 0, "b": 0, "c": 0}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable)
        connObj.close()
        self.assertTrue(result['created'])

    def test_insert_doc_correct_format_by_id(self):
        jsonVariable = {"a": 0, "b": 0, "c": 0}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable,
                                              id=123)
        self.assertTrue(result['created'])
        connObj.close()

    def test_insert_doc_correct_format_by_id_dup(self):
        jsonVariable = {"a": 0, "b": 0, "c": 0}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable,
                                              id=543)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable,
                                              id=543)
        self.assertFalse(result['created'])
        connObj.close()

    def test_delete_doc_existing_id(self):
        jsonVariable = {"a": 0, "b": 0, "c": 0}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_insert_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable,
                                              id=123)
        result = connObj.execute_delete_doc(index='test',
                                            docType='unittest',
                                            docId=123)
        connObj.close()
        self.assertTrue(result['found'])

    def test_delete_doc_no_existing_id(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                result = connObj.execute_delete_doc(index='test',
                                                    docType='unittest',
                                                    docId=555)
            finally:
                connObj.close()

    def test_update_correct(self):
        jsonVariable = {"doc": {"a": 2, "b": 3, "c": 4}}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_update_query(index='test',
                                              docType='unittest',
                                              body=jsonVariable,
                                              id=1)
        connObj.close()
        self.assertGreater(result['_version'], 1)

    def test_update_incorrect_index(self):
        jsonVariable = {"doc": {"a": 3, "b": 3, "c": 3}}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            result = connObj.execute_update_query(index='etest',
                                                  docType='unittest',
                                                  body=jsonVariable,
                                                  id=1)
            connObj.close()

    def test_update_incorrect_doc_type(self):
        jsonVariable = {"doc": {"a": 4, "b": 3, "c": 2}}
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            result = connObj.execute_update_query(index='test',
                                                  docType='eunittest',
                                                  body=jsonVariable,
                                                  id=1)
            connObj.close()

    def test_search_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                connObj.execute_searcU_query("one", "doc", "badExample")
            finally:
                connObj.close()

    def test_search_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_search_query(index='test',
                                              docType='unittest',
                                              body={"query": {"match_all": {}}}
                                              )
        connObj.close()
        self.assertIsNotNone(result)

    def test_create_table_incorrect_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            try:
                result = connObj.execute_create_table("BADTABLE")
                print(result)
            finally:
                connObj.close()

    def test_create_table_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_create_table(index='goodtable')
        connObj.close()
        self.assertIsNotNone(result)

    def test_delete_table_existing_index(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        result = connObj.execute_delete_table(index='goodtable')
        connObj.close()
        self.assertTrue(result['acknowledged'])

    def test_delete_table_no_existing_index(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.GenericError):
            result = connObj.execute_delete_table(index='tester')
            connObj.close()

    def test_create_doctype_incorrect_index(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        with self.assertRaises(EH.InputError):
            try:
                mapping = {
                            "testType" : {
                                "properties" : {
                                  "a" : {
                                    "type" : "long"
                                  },
                                  "b" : {
                                    "type" : "long"
                                  },
                                  "c" : {
                                    "type" : "long"
                                  }
                                }
                            }}
                result = connObj.execute_create_doc_type(index="dontexist",
                                                         docType="test_doc",
                                                         mapping=mapping)
                print(str(result))
            finally:
                connObj.close()
 
    def test_create_doctype_correct_format(self):
        config.configES = configTest.validESConn
        connObj = DBC.U_DBConnection().get_connection(configTest.validESName)
        mapping = {
          "goodtable": {
            "mappings": {
                "map": 2
                          }
          }
        }
        result = connObj.execute_create_doc_type(index="goodtable",
                                                 docType="test_doc",
                                                 mapping=mapping)
        print(result)
        connObj.close()
        self.assertTrue(result['created'])

if __name__ == '__main__':
    UT.main()
