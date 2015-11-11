'''
Created on 5 Nov 2015

@author: Ruben.Alonso
'''

import json
import OA_DB_Connector.DB_Connector as DB
import OA_Exception_Handler.Exception_Handler as EH
import OA_Logging_Handler.Logging_Handler as LH
import H_Query_Engine.QueryInvoker as QE
import config
from datetime import datetime
from dateutil.relativedelta import relativedelta


def process_data(conn, hit, docId, lastDate, untilDate):
    """ Function to process the data with the information received from
    elasticsearch DB.

    Arguments:
        conn: DB connection
        hit: WS provider entry to process
        today: today date
        lastDate: last time the query was executed

    Returns: number of files received from WS provider, number of files which
             match the filter
    """
    myUrl = hit['url']
    query = hit['query']
    provider = hit['name']
    engine = hit['engine']

    # TODO: multi-page should be retrieved from DB
    try:
        queryEngine = QE.H_QueryInvoker.get_engine(engine,
                                                   myUrl,
                                                   lastDate.date(),
                                                   untilDate.date())
    except ValueError as err:
        LH.fileLogger.info("Error retrieving the engine {eng}".format(eng=engine))
        raise err
    try:
        numFilesReceived = queryEngine.execute(conn)
    except Exception as err:
        LH.fileLogger.info("Error retrieving documents from the WS provider \
        {provider}".format(provider=provider))
        raise err
    try:
        result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                           config.TEMPORARY_DOCTYPE_NAME,
                                           query)
    except Exception as err:
        LH.fileLogger.info("Error retrieving documents with the chosen query")
        raise err

    print(result)
    numFilesToSend = result['hits']['total']
    # TODO: Add error checking when we know who to call router
    # TODO: Add resultRouter = router_API.send(result)

    print("Received from " + str(provider) + " " + str(numFilesReceived) + " files")
    print("Sent to router " + str(numFilesToSend) + " files")

    todayTimeStamp = {
                         'doc': {'end_date': int(untilDate.timestamp() * 1000)}
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
    """ Main function of Harvester. It processes the data obtained from the
    different WS provider in accordance with the information stored in the DB
    and applies a filter to the documents. The result of this filter is send
    to the router. Also, it saves historic information about the queries
    applied, filter used, number of documents and if there was a error.

    Arguments:
        conn: DB connection
    """
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
            docId = hit['_id']
            hit = hit["_source"]
            lastDate = datetime.fromtimestamp(hit['end_date']/1000)
            waitWindow = int(hit['wait_window'])
            frequency = hit['frequency']
            if 'daily' == frequency:
                untilDate = today - relativedelta(days=waitWindow)
                if lastDate > untilDate:
                    LH.fileLogger.info("Last time checked was less than a day \
                    ago, exit")
                    continue
            elif 'weekly' == frequency:
                untilDate = today + relativedelta(days=-7) - relativedelta(days=waitWindow)
                if lastDate > untilDate:
                    LH.fileLogger.info("Last time checked was less than a week \
                    ago, exit")
                    continue
            elif 'monthly' == frequency:
                untilDate = today + relativedelta(months=-1) - relativedelta(days=waitWindow)
                if lastDate > untilDate:
                    LH.fileLogger.info("Last time checked was less than a month\
                     ago, exit")
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
                                                                lastDate,
                                                                untilDate)
            except Exception as err:
                print("Error!! " + str(err.message))
                exceptionInfo = err.message
            finally:
                historyInfo = {}
                historyInfo['date'] = int(today.timestamp() * 1000)
                historyInfo['name_ws'] = hit['name']
                historyInfo['url'] = hit['url']
                historyInfo['query'] = hit['query']
                historyInfo['start_date'] = int(hit['end_date'])
                historyInfo['end_date'] = int(untilDate.timestamp() * 1000)
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
        # conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
        DB.H_DBConnection().del_connection()
