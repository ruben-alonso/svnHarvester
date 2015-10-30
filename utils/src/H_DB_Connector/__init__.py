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

    __conn = None

    def get_connection(self, typeDB):
        LH.fileLogger.info("Getting DB connection")
        if self.__conn is None:
            if "elastic_search_v1" == typeDB:
                LH.fileLogger.info("ElasticSearch connection")
                __conn = _ElasticSearchV1(self)
                return __conn
            if "mySQL_V1" == typeDB:
                LH.fileLogger.info("MySQL connection")
                __conn = 1  #MySQLV1(self)
                return __conn
            raise EH.InputError("Incorrect BD name: " + typeDB)
        else:
            LH.fileLogger.info("Connection exists")
            return __conn

    def del_connection(self):

        if self.__conn is not None:
            self.__conn.close()


class AbstractConnector(with_metaclass(ABCMeta)):
    """ Abstract class for the different connectors we might use"""

    @abstractclassmethod
    def execute_search_query(self, index, docType, body):
        """ Abstract method for searching on a DB"""
        pass

    @abstractclassmethod
    def execute_create_table(self, index):
        """ Abstract method for creating a new table"""
        pass

    @abstractclassmethod
    def execute_create_doc_type(self, index, docTypeName, mapping):
        """ Abstract method for creating a new table"""
        pass

    @abstractclassmethod
    def execute_insert_query(self, index, docType, body):
        """ Abstract method for inserting new data in the DB"""
        pass

    @abstractclassmethod
    def execute_update_query(self, index, docType, docId, body):
        """ Abstract method for updating a register by ID"""
        pass

    @abstractclassmethod
    def execute_delete_table(self, index):
        """ Abstract method for deleting the content of a table"""
        pass

    @abstractclassmethod
    def execute_delete_id(self, index, docId):
        """ Abstract method for deleting a register by ID"""
        pass

    @abstractclassmethod
    def close(self):
        """ Method to close the connection to the DB"""
        pass


class _ElasticSearchV1(AbstractConnector):
    """ Creates a DB connector to the ES database and provides different
        queries to manage the content"""

    def __init__(self, fact):
        """Constructor to initialize the DB connector"""
        self.fact = fact
        self.connector = ES.Elasticsearch(config.configES)
        try:
            with nostderr():
                self.connector.ping()
        except ES.ConnectionTimeout as err:
            raise EH.DBConnectionError(message="Connection timeout error",
                                       error=str(err))
        except ConnectionRefusedError as err:
            raise EH.DBConnectionError(message="Connection refused error",
                                       error=str(err))
        except urllib3.exceptions.NewConnectionError as err:
            raise EH.DBConnectionError(message="New connection error",
                                       error=str(err))
        except ES.ConnectionError as err:
            raise EH.DBConnectionError(message="Generic connection error",
                                       error=str(err))
        except Exception as err:
            raise EH.DBConnectionError(message="Other connection error",
                                       error=str(err))

    def execute_search_query(self, index, docType, body):
        self.connector.indices.refresh()
        response = self.connector.search(index=index, doc_type=docType,
                                         body=body)
        return response

    def execute_create_table(self, index):
        response = self.connector.create_index(index)
        return response  #do we need to check the response in here or in the function calling??

    def execute_create_doc_type(self, index, docTypeName, mapping):
        response = self.connector.put_mapping(docTypeName,
                                              {'properties': mapping}, [index])
        return response  #do we need to check the response in here or in the function calling??

    def execute_insert_query(self, index, docType, body):
        response = self.connector.create(index=index, doc_type=docType,
                                         body=body)
        return response

    def execute_update_query(self, index, docType, body):
        response = self.connector.index(index=index, doc_type=docType,
                                        body=body)
        return response

    def execute_delete_table(self, index):
        self.connector.delete_index(index)

    def execute_delete_id(self, index, docId):
        self.connector.delete_index(index)

    def close(self):
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

jsonVariable = {"a": 0, "b": 0, "c": 0}
try:
    connObj = H_DBConnection().get_connection('elastic_search_v1')
    if connObj.connector is not None:
        # connObj.execute_insert_query(index='test', docType='testType', body=jsonVariable)
        response = connObj.execute_search_query(index='test', docType=None, 
                                                body={"query": {"match_all" : { }}})

        print("Got %d Hits:" % response['hits']['total'])
        for hit in response['hits']['hits']:
            print(hit["_source"])

        connObj.close()
except Exception as err:
    print(err)
    print(err.message)

# import json, requests
# import elasticsearch as ES
#  
# es = ES.Elasticsearch(config.configES)
# print(es)
# r = requests.get('http://10.100.13.46:9200')
# print(r.content)
# i = 8
# while r.status_code == 200 and 10>i:
#     r = requests.get('http://swapi.co/api/people/'+ str(i))
#     es.index(index='sw', doc_type='people', id=i, body=json.loads(r.content.decode('utf-8')))
#     i=i+1
# print(i)