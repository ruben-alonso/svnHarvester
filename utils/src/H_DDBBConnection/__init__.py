'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
import config
from abc import abstractclassmethod, ABCMeta
from flask._compat import with_metaclass


class H_DDBBConnection(object):

    @classmethod
    def factory(typeDB):
        if typeDB == "elastic_search_v1":
            return ElasticSearchV1()
        if typeDB == "mySQL_V1":
            return MySQLV1()
        assert 0, "Bad DDBB connection creation: " + type


class AbstractConnector(with_metaclass(ABCMeta)):
    """ Abstract class for the different connectors we might use"""

    @abstractclassmethod
    def __init__(self):
        pass

    @classmethod
    def connector(self):
        return self.connector

    @classmethod
    def close(self):
        self.connector.close()


class ElasticSearchV1(AbstractConnector):
    """ Creates a DDBB connector to the ES database and provides different
        functions to manage it"""
    def __init__(self):
        from elasticsearch import Elasticsearch
        import elasticsearch as ES
        try:
            self.connector = Elasticsearch(config.configES)
        except ES.ConnectionError as err:
            print("Connection error" + err)
        except ES.ConnectionTimeout as err:
            print("Connection timeout" + err)
        else:
            self.connector.close()


class MySQLV1(AbstractConnector):
    """ Creates a DDBB connector to the MySQL database and provides different
        functions to manage it"""
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
            self.connector.close()
