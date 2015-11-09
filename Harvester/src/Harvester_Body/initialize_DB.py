'''
Created on 9 Nov 2015

@author: Ruben.Alonso
'''
import json
import OA_DB_Connector.DB_Connector as DB
import config
import time


def main():
    print("Let's put some data in the DB")
    conn = DB.H_DBConnection().get_connection(config.DB_NAME)

    print("Let's clean the tables before starting")
    try:
        conn.execute_delete_table(config.HISTORY_INDEX_NAME)
        conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
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
    webservices['url'] = "http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=xml&pageSize=1000&query=%20CREATION_DATE%3A%5B{startDate}%20TO%20{endDate}%5D"
    webservices['query'] = json.dumps({"query" : {"constant_score" : {"filter" : {"exists" : {"field" : "authorlist.author.affiliation"}}}}})
    webservices['frequency'] = "daily"
    webservices['active'] = True
    webservices['email'] = "ruben.alonso@jisc.ac.uk"
    webservices['last_date'] = int(time.time() * 1000)

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
