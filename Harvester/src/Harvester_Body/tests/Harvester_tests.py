'''
Created on 11 Nov 2015

@author: Ruben.Alonso
'''
import json
import OA_DB_Connector.DB_Connector as DB
import config
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import unittest
import Harvester_Body.Harvester as HB


def clean_and_create(conn):

    try:
        conn.execute_delete_table(config.WEBSERVICES_INDEX_NAME)
    except Exception as err:
        print(str(err))
    try:
        conn.execute_delete_table(config.HISTORY_INDEX_NAME)
    except Exception as err:
        print(str(err))

    conn.execute_create_table(config.HISTORY_INDEX_NAME)
    conn.execute_create_table(config.WEBSERVICES_INDEX_NAME)

    conn.execute_create_doc_type(config.WEBSERVICES_INDEX_NAME,
                                 config.WEBSERVICES_DOCTYPE_NAME,
                                 config.WEBSERVICES_MAPPING)
    conn.execute_create_doc_type(config.HISTORY_INDEX_NAME,
                                 config.HISTORY_DOCTYPE_NAME,
                                 config.HISTORY_MAPPING)


def wrong_engine_name(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)


def no_active_provider(conn):
    """ Function to initialise the DB with no active providers
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = False
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(time.time() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 1

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def no_provider(conn):
    """ Function to initialise the DB with no providers
    """
    clean_and_create(conn)


def wrong_frequency_name(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "never please"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(time.time() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 1

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def daily_with_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": { "field": "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(untilDate.timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['active'] = False
    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['active'] = False
    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def several_provider_some_wrong(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": { "field": "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(untilDate.timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['active'] = True
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/sarch/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/ret/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['active'] = True
    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def several_provider_wrong_query(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "fter": {
                    "exists": { "field": "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(untilDate.timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": { "fild": "authorList.author.affiliation" }
                }
            }
        }
                            })
    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def daily_without_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(datetime.today().timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def weekly_with_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "weekly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=11)
    webservices['end_date'] = int(untilDate.timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 4

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def weekly_without_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "weekly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(datetime.today().timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def monthly_with_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "monthly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(month=2)
    webservices['end_date'] = int(untilDate.timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 29

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


def monthly_without_data(conn):
    """ Function to initialise the DB with an incorrect engine name
    """
    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : { "field" : "authorList.author.affiliation" }
                }
            }
        }
                            })
    webservices['frequency'] = "monthly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(datetime.today().timestamp() * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)


class HarvesterTest(unittest.TestCase):

    def test_wrong_engine_name(self):
        with self.assertRaises(ValueError):
            conn = DB.H_DBConnection().get_connection(config.DB_NAME)
            wrong_frequency_name(conn)
            hit = {}
            hit['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
            hit['query'] = json.dumps({
                "query" : {
                    "filtered" : {
                        "filter" : {
                            "exists" : { "field" : "authorList.author.affiliation" }
                        }
                    }
                }
                                })
            hit['name'] = "EPMC"
            hit['engine'] = "BAD_ENGINE"
            HB.process_data(conn, hit, 1, datetime.today(), datetime.today())
            DB.H_DBConnection().del_connection()

    def test_no_active_provider(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        no_active_provider(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual("No queries, bye bye", result)

    def test_no_provider(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        no_active_provider(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual("No queries, bye bye", result)

    def test_daily_with_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        daily_with_data(conn)
        HB.main(conn)
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.H_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 1)

    def test_daily_without_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        daily_without_data(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual(0, result)

    # TODO: check error inside result
    def test_several_provider_wrong_url(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        several_provider_some_wrong(conn)
        HB.main(conn)
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.H_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 3)

    # TODO: check error message
    def test_several_provider_wrong_query(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        several_provider_wrong_query(conn)
        HB.main(conn)
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.H_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 2)

    def test_weekly_with_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        weekly_with_data(conn)
        HB.main(conn)
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.H_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 2)

    def test_weekly_without_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        weekly_without_data(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual(0, result)

    def test_monthly_with_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        monthly_with_data(conn)
        HB.main(conn)
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.H_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 3)

    def test_monthly_without_data(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        monthly_without_data(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual(0, result)

    def test_wrong_frequency_name(self):
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        wrong_frequency_name(conn)
        result = HB.main(conn)
        DB.H_DBConnection().del_connection()
        self.assertEqual(0, result)

if __name__ == "__main__":
    unittest.main()
