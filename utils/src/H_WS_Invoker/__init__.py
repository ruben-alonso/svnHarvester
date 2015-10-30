'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
import json
import config
import requests
import H_Exception_Handler as EH
import xml.etree.ElementTree as ET


class H_WSInvoker (object):
    """Web services invoker to retrieve all information from a specific source
    and with specific conditions.
    """

    def __retrieve_information(self, query):
        """Generic function which runs the query previously created
        """
        return requests.get(query)

    def retrieve_information_xml(self, query):
        """Returns the information obtained from the specific source and
        according to the parameters.
        """
        returnedRequest = self.__retrieve_information(self, query)

        try:
            xmlRequest = ET.fromstring(returnedRequest.text)
        except ET.ParseError:
            raise EH.IncorrectFormatError("Not well-formed", "The output format is\
                                        incorrect, review ")
        return xmlRequest

    def retrieve_information_json(self, query):
        """Returns the information obtained from the specific source and
        according to the parameters.
        """
        returnedRequest = self.__retrieve_information(self, query)
        try:
            json.loads(returnedRequest.text)
        except json.decoder.JSONDecodeError:
            raise EH.IncorrectFormatError("Not well-formed", "The output format is\
                                        incorrect, review ")

        return ET.fromstring(returnedRequest.text)
