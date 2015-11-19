'''
Created on 11 Nov 2015

@author: Ruben.Alonso
'''
import json
import utils.connector.connector as DB
import utils.logger.handler as LH
import utils.config as config
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import unittest
import harvester.body.Harvester as HB


def to_timestamp(dt, epoch=datetime(1970, 1, 1)):
    td = dt - epoch
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6


def clean_and_create(conn):
    """Function to remove existing data from the DB and initialise it with
    indices and mapping for the Harvester structure
    """
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
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
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
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
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    webservices['active'] = False
    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    webservices['active'] = False
    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)


def several_provider_some_wrong(conn):
    """ Function to initialise the DB with incorrect providers
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    webservices['active'] = True
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/sarch/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/ret/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['active'] = True
    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)


def several_provider_wrong_query(conn):
    """ Function to initialise the DB with an incorrect query
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "fter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=1)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"fild": "authorList.author.affiliation"}
                }
            }
        }
                            })
    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)


def daily_without_data(conn):
    """ Function to initialise the DB without time to execute
    """

    clean_and_create(conn)

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
    webservices['query'] = json.dumps({
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(to_timestamp(datetime.today()) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "weekly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today - relativedelta(days=11)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 4

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)

    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "weekly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(to_timestamp(datetime.today()) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "monthly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    today = datetime.today()
    untilDate = today + relativedelta(months=-2)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 29

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
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
        "query": {
            "filtered": {
                "filter": {
                    "exists": {"field": "authorList.author.affiliation"}
                }
            }
        }
                            })
    webservices['frequency'] = "monthly"
    webservices['active'] = True
    webservices['email'] = "ruben@mio.mine"
    webservices['end_date'] = int(to_timestamp(datetime.today()) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 0

    json_webservices = json.dumps(webservices)
    conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                              config.WEBSERVICES_DOCTYPE_NAME,
                              json_webservices)


class H_HarvesterTest(unittest.TestCase):

    def test_wrong_engine_name(self):
        LH.fileLogger.info("Test_wrong engine name")
        with self.assertRaises(ValueError):
            conn = DB.U_DBConnection().get_connection(config.DB_NAME)
            wrong_frequency_name(conn)
            hit = {}
            hit['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
            hit['query'] = json.dumps({
                "query": {
                    "filtered": {
                        "filter": {
                            "exists": {"field": "authorList.author.affiliation"}
                        }
                    }
                }
                                })
            hit['name'] = "EPMC"
            hit['engine'] = "BAD_ENGINE"
            HB.process_data(conn, hit, 1, datetime.today(), datetime.today())
            DB.U_DBConnection().del_connection()

    def test_no_active_provider(self):
        LH.fileLogger.info("Test_no_active_provider")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        no_active_provider(conn)
        result = HB.main(conn)
        DB.U_DBConnection().del_connection()
        self.assertEqual("No queries, bye bye", result)

    def test_no_provider(self):
        LH.fileLogger.info("Test_no_provider")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        no_active_provider(conn)
        result = HB.main(conn)
        DB.U_DBConnection().del_connection()
        self.assertEqual("No queries, bye bye", result)

    def test_daily_with_data(self):
        LH.fileLogger.info("Test_daily with data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
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
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 1)

    def test_daily_without_data(self):
        LH.fileLogger.info("Test_daily without data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        daily_without_data(conn)
        HB.main(conn)
        DB.U_DBConnection().del_connection()
        query = {
            "query": {
                "match_all": {}
            }
        }
        result = conn.execute_search_query(config.HISTORY_INDEX_NAME,
                                           config.HISTORY_DOCTYPE_NAME,
                                           query)
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['hits'][0]['_source']['num_files_received'], 0)

    # TODO: check error inside result
    def test_several_provider_wrong_url(self):
        LH.fileLogger.info("Test_several provider wrong url")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
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
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 3)

#     # TODO: check error message
    def test_several_provider_wrong_query(self):
        LH.fileLogger.info("Test_several provider wrong name")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
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
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 2)

    def test_weekly_with_data(self):
        LH.fileLogger.info("Test_weekly with data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
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
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 2)

    def test_weekly_without_data(self):
        LH.fileLogger.info("Test_weekly without data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        weekly_without_data(conn)
        result = HB.main(conn)
        DB.U_DBConnection().del_connection()
        self.assertEqual(0, result)

    def test_monthly_with_data(self):
        LH.fileLogger.info("Test_monthly with data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
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
        DB.U_DBConnection().del_connection()
        self.assertEqual(result['hits']['total'], 3)

    def test_monthly_without_data(self):
        LH.fileLogger.info("Test_monthly without data")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        monthly_without_data(conn)
        result = HB.main(conn)
        DB.U_DBConnection().del_connection()
        self.assertEqual(0, result)

    def test_wrong_frequency_name(self):
        LH.fileLogger.info("Test_wrong frequency name")
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        wrong_frequency_name(conn)
        result = HB.main(conn)
        DB.U_DBConnection().del_connection()
        self.assertEqual(0, result)

if __name__ == "__main__":
    unittest.main()
