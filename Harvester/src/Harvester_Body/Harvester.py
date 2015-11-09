'''
Created on 5 Nov 2015

@author: Ruben.Alonso
'''

import json
import OA_DB_Connector.DB_Connector as DB
import OA_Exception_Handler.Exception_Handler as EH
import OA_Logging_Handler.Logging_Handler as LH
# TODO: To be changed to the new engine import ImportEPMC.H_ImportEPMC as QE  
import config
from datetime import datetime
from dateutil import relativedelta


def process_data(conn, hit, docId, today, lastDate):

    myUrl = hit['url']
    query = hit['query']
    provider = hit['name']

    # TODO: When the query engine exists
    # queryEngine = QE.query_engine(provider)
    # numFilesReceived = queryEngine.import_all(myUrl, today, lastDate)
    numFilesReceived = 3
    try:
        result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                           config.TEMPORARY_DOCTYPE_NAME,
                                           query)
    except Exception as err:
        LH.fileLogger.info("Error retrieving documents with the chosen query")
        raise err

    numFilesToSend = result['hits']['total']
    # TODO: Add error checking when we know who to call router
    # TODO: Add resultRouter = router_API.send(result)
    todayTimeStamp = {
                         'doc': {'last_date': int(today.timestamp() * 1000)}
                     }
    try:

        result = conn.execute_update_query(config.WEBSERVICES_INDEX_NAME,
                                           config.WEBSERVICES_DOCTYPE_NAME,
                                           docId,
                                           todayTimeStamp)
        LH.fileLogger.info("Updated last execution date")
    except EH.Error as err:
        print(str(err))
        LH.fileLogger.error("Execution date update failed",
                      str(err))
        raise err

    return numFilesReceived, numFilesToSend


def main(conn):

    conn.execute_create_table(config.TEMPORARY_INDEX_NAME)

    try:
        query = {
            "query": {
                "match": {
                    "active": True
                }
            }
        }
        result = conn.execute_search_query(config.WEBSERVICES_INDEX_NAME,
                                           config.WEBSERVICES_DOCTYPE_NAME,
                                           query)
    except Exception as err:
        LH.fileLogger.info("The query to the DB failed, exit")
        return "It has failed to retrieve the active queries..."

    hits = result['hits']['hits']
    if not hits:
        LH.fileLogger.info('No matches found, exit')
        return "No queries, bye bye"
    else:
        exceptionInfo = None
        numFilesReceived, numFilesToSend = 0, 0
        today = datetime.today()
        for hit in hits:
            print(hit)
            docId = hit['_id']
            hit = hit["_source"]
            lastDate = datetime.fromtimestamp(hit['last_date']/1000).strftime('%Y-%m-%d')
            frequency = hit['frequency']
            if 'daily' == frequency:
                if today == lastDate:
                    LH.fileLogger.info("Last time checked was today, exit")
                    continue
            elif 'weekly' == frequency:
                passWeekDate = today + relativedelta(days=-7)
                if lastDate < passWeekDate:
                    LH.fileLogger.info("Last time checked was less than a week \
                    before, exit")
                    continue
            elif 'monthly' == frequency:
                passMonthDate = today + relativedelta(months=-1)
                if lastDate < passMonthDate:
                    LH.fileLogger.info("Last time checked was less than a month\
                     before, exit")
                    continue
            else:
                LH.fileLogger.error("Incorrect value error", "The value\
                 frequency retrieved from the DB is incorrect, please \
                 fix it to continue")
                continue
            try:
                numFilesReceived, numFilesToSend = process_data(conn,
                                                                hit,
                                                                docId,
                                                                today,
                                                                lastDate)
            except Exception as err:
                print("Error!!" + str(err))
                exceptionInfo = err
            finally:
                historyInfo = {}
                historyInfo['date'] = int(today.timestamp() * 1000)
                historyInfo['name_ws'] = hit['name']
                historyInfo['url'] = hit['url']
                historyInfo['query'] = hit['query']
                historyInfo['num_files_received'] = numFilesReceived
                historyInfo['num_files_sent'] = numFilesToSend
                historyInfo['error'] = str(exceptionInfo)

                json_history = json.dumps(historyInfo)

                try:
                    result = conn.execute_insert_query(config.HISTORY_INDEX_NAME,
                                                       config.HISTORY_DOCTYPE_NAME,
                                                       json_history)
                    LH.fileLogger.info("Historic entry added to the DB")
                except EH.Error as err:
                    LH.fileLogger.error("Historic entry failed to be added to the DB",
                                  err.message)
                    return "has failed because..."


if __name__ == '__main__':

    try:
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)
        main(conn)
    except EH.DBConnectionError as err:
        LH.fileLogger.info("The connection to the database failed, exit")
        exit
    finally:
        conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
        DB.H_DBConnection().del_connection()
