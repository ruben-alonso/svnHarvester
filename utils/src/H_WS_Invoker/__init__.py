'''
Created on 26 Oct 2015

@author: Ruben.Alonso

@Description: Module created for web services invocation.
'''
import json
import requests
import H_Exception_Handler as EH
import H_Logging_Handler as LH
import xml.etree.ElementTree as ET

class H_WSInvoker (object):
    """Web services invoker to retrieve all information from a specific source
    and with specific conditions.
    """

    def __retrieve_information(self, query):
        """Generic function which runs the query previously created

        Attributes:
            query -- query to execute to the ws system
        """
        return requests.get(query)

    def retrieve_information_xml(self, query):
        """Returns the information obtained from the specific source and
        according to the parameters.

        Attributes:
            query -- query to execute to the ws system
        """
        LH.fileLogger.info("Retrieving xml information")
        try:
            returnRequest = self.__retrieve_information(self, query)
        except requests.exceptions.HTTPError as err:
            raise EH.GenericError("Invalid HTTP response", err)
        except requests.exceptions.Timeout as err:
            raise EH.GenericError("Timeout", err)
        except requests.exceptions.RequestException as err:
            raise EH.GenericError("Request exception", err)
        try:
            xmlRequest = ET.fromstring(returnRequest.text)
        except ET.ParseError:
            raise EH.IncorrectFormatError("Not well-formed", "The output format is\
                                        not xml, please review ")
        return xmlRequest

    def retrieve_information_json(self, query):
        """Returns the information obtained from the specific source and
        according to the parameters.

        Attributes:
            query -- query to execute to the ws system
        """
        LH.fileLogger.info("Retrieving json information")
        try:
            returnRequest = self.__retrieve_information(self, query)
        except requests.exceptions.HTTPError as err:
            raise EH.GenericError("Invalid HTTP response", err)
        except requests.exceptions.Timeout as err:
            raise EH.GenericError("Timeout", err)
        except requests.exceptions.RequestException as err:
            raise EH.GenericError("Request exception", err)
        try:
            json.loads(returnRequest.text)
        except json.decoder.JSONDecodeError:
            raise EH.IncorrectFormatError("Not well-formed", "The output format is\
                                        not xml, please review ")

        return ET.fromstring(returnRequest.text)
