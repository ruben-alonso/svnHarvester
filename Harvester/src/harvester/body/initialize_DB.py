'''
Created on 9 Nov 2015

@author: Ruben.Alonso
'''
from __future__ import division
import json
import utils.connector.connector as DB
import utils.config as config
from datetime import datetime
from dateutil.relativedelta import relativedelta


def to_timestamp(dt, epoch=datetime(1970, 1, 1)):
    td = dt - epoch
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6


def main():
    """ Function to create a initial instance of the DB for the Harvester to
    start working. It creates the DB indices, the document mappings and insert
    initial information for the existing WS provider.
    """
    print("Let's put some data in the DB")
    conn = DB.U_DBConnection().get_connection(config.DB_NAME)

    print("Let's clean the tables before starting")
    try:
        conn.execute_delete_table(config.HISTORY_INDEX_NAME)
    except Exception as err:
        print(str(err))
    try:
        conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
    except Exception as err:
        print(str(err))
    try:
        conn.execute_delete_table(config.WEBSERVICES_INDEX_NAME)
    except Exception as err:
        print(str(err))

    print("Creating indices for DB model")
    conn.execute_create_table(config.TEMPORARY_INDEX_NAME)
    conn.execute_create_table(config.HISTORY_INDEX_NAME)
    conn.execute_create_table(config.WEBSERVICES_INDEX_NAME)

    print("Creating mapping for DB model")
    conn.execute_create_doc_type(config.HISTORY_INDEX_NAME,
                                 config.HISTORY_DOCTYPE_NAME,
                                 config.HISTORY_MAPPING)
    conn.execute_create_doc_type(config.WEBSERVICES_INDEX_NAME,
                                 config.WEBSERVICES_DOCTYPE_NAME,
                                 config.WEBSERVICES_MAPPING)


    print("Inserting webservices information")

    webservices = {}
    webservices['name'] = "EPMC"
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&pageSize=1000&query=26566832&%20CREATION_DATE%3A%5B{start_date}%20TO%20{end_date}%5D"
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
    webservices['email'] = "ruben.alonso@jisc.ac.uk"
    today = datetime.today()
    untilDate = today - relativedelta(days=3)
    webservices['end_date'] = int(to_timestamp(untilDate) * 1000)
    webservices['engine'] = config.MULTI_PAGE
    webservices['wait_window'] = 2

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)

    webservices['name'] = "PMC"
    webservices['frequency'] = "weekly"
    webservices['active'] = False
    webservices['email'] = "ruben.alonso@jisc.ac.uk"

    json_webservices = json.dumps(webservices)
    result = conn.execute_insert_query(config.WEBSERVICES_INDEX_NAME,
                                       config.WEBSERVICES_DOCTYPE_NAME,
                                       json_webservices)
if __name__ == '__main__':
    main()
