'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
import config
import sys
import contextlib
import json
import urllib3
import elasticsearch as ES
import H_Exception_Handler as EH
import H_Logging_Handler as LH
from abc import abstractclassmethod, ABCMeta
from flask._compat import with_metaclass


@contextlib.contextmanager
def nostderr():
    """ Function necessary to remove unwanted log information from stdout and
    stderr
    """
    savestderr = sys.stderr

    class Devnull(object):
        def write(self, _): pass

        def flush(self): pass
    sys.stderr = Devnull()
    try:
        yield
    finally:
        sys.stderr = savestderr


class H_DBConnection(object):
    """ Factory class to select among all the possible DB we can provide"""
    __conn = None

    def get_connection(self, typeDB):
        """Factory function which chooses among the possible DB

        Attributes:
            typeDB -- select which DB is gonig to be created by the factory
                      function
        """
        LH.fileLogger.info("Getting DB connection")
        if self.__conn is None:
            if "elastic_search_v1" == typeDB:
                LH.fileLogger.info("ElasticSearch connection")
                __conn = _ElasticSearchV1(self)
                return __conn
            if "mySQL_V1" == typeDB:
                LH.fileLogger.info("MySQL connection")
                __conn = 1  # MySQLV1(self)
                return __conn
            raise EH.InputError("Incorrect BD name: " + typeDB)
        else:
            LH.fileLogger.info("Returning existing connection")
            return __conn

    def del_connection(self):
        """Function to close the connection when one exists"""
        LH.fileLogger.info("Closing the connection")
        if self.__conn is not None:
            self.__conn.close()


class AbstractConnector(with_metaclass(ABCMeta)):
    """ Abstract class for the different connectors we might use"""

    @abstractclassmethod
    def execute_search_query(self, index, docType, body):
        """ Abstract method for searching on a DB

        Attributes:
            index -- table to run the query to
            docType -- type of document target of the query
            body -- query to execute
        """
        pass

    @abstractclassmethod
    def execute_create_table(self, index):
        """ Abstract method for creating a new table

        Attributes:
            index -- table to be created
        """
        pass

    @abstractclassmethod
    def execute_create_doc_type(self, index, docType, mapping):
        """ Abstract method for creating a new type of document

        Attributes:
            index -- table to run the query to
            docType -- type of document to be created
            mapping -- structure of the document
        """
        pass

    @abstractclassmethod
    def execute_insert_query(self, index, docType, body):
        """ Abstract method for inserting new data in the DB

        Attributes:
            index -- table to run the query to
            docType -- type of document target of the query
            body -- query to execute
        """
        pass

    @abstractclassmethod
    def execute_update_query(self, index, docType, docId, body):
        """ Abstract method for updating a register by ID

        Attributes:
            index -- table to run the query to
            docType -- type of document target of the query
            docId -- doc to be updated
            body -- information to be updated to the specified document
        """
        pass

    @abstractclassmethod
    def execute_delete_table(self, index):
        """ Abstract method for deleting the content of a table

        Attributes:
            index -- table to run the delete command
        """
        pass

    @abstractclassmethod
    def execute_delete_id(self, index, docId):
        """ Abstract method for deleting a register by ID

        Attributes:
            index -- table to run the delete command
            docId -- ID of the document to be deleted
        """
        pass

    @abstractclassmethod
    def close(self):
        """ Method to close the connection to the DB"""
        pass


class _ElasticSearchV1(AbstractConnector):
    """ Creates a DB connector to the ES database and provides different
        queries to manage the content"""

    def __init__(self, fact):
        """Constructor to initialize the DB connector

        Raises DBConnectionError
        """
        self.fact = fact
        self.connector = ES.Elasticsearch(config.configES)
        try:
            with nostderr():
                self.connector.ping()
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ConnectionRefusedError as err:
            raise EH.DBConnectionError(message="Connection refused error",
                                       error=str(err))
        except urllib3.exceptions.NewConnectionError as err:
            raise EH.DBConnectionError(message="New connection error",
                                       error=str(err))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except Exception as err:
            raise EH.DBConnectionError(message="Other connection error",
                                       error=str(err.info))

    def execute_search_query(self, index, docType, body):
        LH.fileLogger.info("Executing search query") # How much information do we need here: query, table, etc.
        try:
            response = self.connector.search(index=index, doc_type=docType,
                                             body=body)
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while searching",
                                  error=str(err.info))
        return response

    def execute_create_table(self, index):
        LH.fileLogger.info("Creating a new table {0}".format(index))
        try:
            response = self.connector.create_index(index)
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while creating a new \
                                  table", error=str(err.info))
        return response  #do we need to check the response in here or in the function calling??

    def execute_create_doc_type(self, index, docType, mapping):
        LH.fileLogger.info("Creating new doc type {0}".format(docType))
        try:
            response = self.connector.put_mapping(docType,
                                                  {'properties': mapping},
                                                  [index])
            self.connector.indices.refresh(index=index)
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while creating a document \
                                  type", error=str(err.info))
        return response  #do we need to check the response in here or in the function calling??

    def execute_insert_query(self, index, docType, body):
        LH.fileLogger.info("Inserting new information into the DB")
        try:
            response = self.connector.create(index=index, doc_type=docType,
                                             body=body)
            self.connector.indices.refresh(index=index)
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while inserting",
                                  error=str(err.info))
        return response

    def execute_update_query(self, index, docType, body):
        LH.fileLogger.info("Updating information from the DB")
        try:
            response = self.connector.index(index=index, doc_type=docType,
                                            body=body)
            self.connector.indices.refresh(index=index)
            return response
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while updating a document",
                                  error=str(err.info))

    def execute_delete_table(self, index):
        LH.fileLogger.info("Deleting table {0}".format(index))
        try:
            return self.connector.delete_index(index)
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while deleting a table",
                                  error=str(err.info))

    def execute_delete_id(self, index, docId):
        LH.fileLogger.info("Deleting document {0}".format(docId))
        try:
            response = self.connector.delete_index(index)
            self.connector.indices.refresh(index=index)
            return response
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err.info))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err.info))
        except ES.ElasticsearchException as err:
            raise EH.GenericError(message="Exception while deleting a document",
                                  error=str(err.info))

    def close(self):
        LH.fileLogger.info("Closing the connection to the DB")
        self.fact.del_connection()

"""class _MySQLV1(AbstractConnector):
     Creates a DB connector to the MySQL database and provides different
        functions to manage it

    def __init__(self):
        import mysql.connector
        from mysql.connector import errorcode
        try:
            self.connector = mysql.connector.connect(**config.configMySQL)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            H_DBConnection.close()
"""

# jsonVariable = {"a": 0, "b": 0, "c": 0}
# try:
#     connObj = H_DBConnection().get_connection('elastic_search_v1')
#     if connObj.connector is not None:
#         # connObj.execute_insert_query(index='test', docType='testType', body=jsonVariable)
#         response = connObj.execute_search_query(index='test', docType=None,
#                                                 body={"query": {"match_all" : { }}})
# 
#         print("Got %d Hits:" % response['hits']['total'])
#         for hit in response['hits']['hits']:
#             print(hit["_source"])
# 
#         connObj.close()
# except Exception as err:
#     print(err)
#     print(err.message)
