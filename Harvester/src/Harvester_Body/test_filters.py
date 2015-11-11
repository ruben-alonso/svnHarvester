'''
Created on 11 Nov 2015

@author: Ruben.Alonso
'''

import json
import OA_DB_Connector.DB_Connector as DB
import OA_Exception_Handler.Exception_Handler as EH
import OA_Logging_Handler.Logging_Handler as LH
import Query_Engine.QueryInvoker as QE
import config
from datetime import datetime
from dateutil.relativedelta import relativedelta


if __name__ == '__main__':
    conn = DB.H_DBConnection().get_connection(config.DB_NAME)

    query = {
    "query" : {
        "filtered" : {
            "filter" : {
                "exists" : { "field" : "authorList.author.affiliation" }
            }
        }
    }
}
    print(query)
    #query={"query" : {"match_all" : {}}}
    query = json.dumps(query)
    result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                       config.TEMPORARY_DOCTYPE_NAME,
                                       query)
    print(result['hits']['total'])
    print(result['hits']['hits'][1])

    query = json.dumps({"query" : {"constant_score" : {"filter" : {"exists" : {"field" : "authorlist"}}}}})
    print(query)
    result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                       config.TEMPORARY_DOCTYPE_NAME,
                                       query)

    print(result)
    DB.H_DBConnection().del_connection()