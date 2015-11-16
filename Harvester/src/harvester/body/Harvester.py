'''
Created on 5 Nov 2015

@author: Ruben.Alonso
'''
from __future__ import division
import json
import utils.connector.connector as DB
import utils.exception.handler as EH
import utils.logger.handler as LH
import engine.query.QueryInvoker as QE
import utils.config as config
from datetime import datetime
from dateutil.relativedelta import relativedelta


def to_timestamp(dt, epoch=datetime(1970, 1, 1)):
    td = dt - epoch
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6


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
                                                   conn,
                                                   myUrl,
                                                   lastDate.date(),
                                                   untilDate.date())
    except ValueError as err:
        LH.fileLogger.error("Incorrect engine {eng}".format(eng=engine))
        raise err
    try:
        numFilesReceived = queryEngine.execute()
    except Exception as err:
        LH.fileLogger.error("Error retrieving documents from the WS provider \
        {provider}".format(provider=provider))
        raise err
    try:
        result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                           config.TEMPORARY_DOCTYPE_NAME,
                                           query)
    except Exception as err:
        LH.fileLogger.error("Error retrieving documents with the chosen query")
        raise err

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
        LH.fileLogger.error("Execution date update failed",
                            str(err))
        raise err
    # We need to clean the DB after finishing one provider so we don't duplicate data
    conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
    conn.execute_create_table(config.TEMPORARY_INDEX_NAME)

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
    try:
        conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
        conn.execute_create_table(config.TEMPORARY_INDEX_NAME)
    except Exception as err:
        return "Something fail with the cleaning "

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
        numProviders = 0
        for hit in hits:
            docId = hit['_id']
            hit = hit["_source"]
            lastDate = datetime.fromtimestamp(hit['end_date']/1000)
            waitWindow = int(hit['wait_window'])
            frequency = hit['frequency']
            if 'daily' == frequency:
                untilDate = today - relativedelta(days=waitWindow)
                if lastDate.date() > untilDate.date():
                    LH.fileLogger.info("Last time checked was less than a day \
                    ago, exit")
                    continue
            elif 'weekly' == frequency:
                untilDate = today + relativedelta(days=-7) - relativedelta(days=waitWindow)
                if lastDate.date() > untilDate.date():
                    LH.fileLogger.info("Last time checked was less than a week \
                    ago, exit")
                    continue
            elif 'monthly' == frequency:
                untilDate = today + relativedelta(months=-1) - relativedelta(days=waitWindow)
                if lastDate.date() > untilDate.date():
                    LH.fileLogger.info("Last time checked was less than a month\
                     ago, exit")
                    continue
            else:
                LH.fileLogger.error("Incorrect value error. The value\
                 frequency retrieved from the DB is incorrect, please \
                 fix it to continue")
                continue

            numProviders += 1
            try:
                numFilesReceived, numFilesToSend = process_data(conn,
                                                                hit,
                                                                docId,
                                                                lastDate,
                                                                untilDate - relativedelta(days=1))
            except Exception as err:
                exceptionInfo = err.message
            finally:
                historyInfo = {}
                historyInfo['date'] = int(to_timestamp(today) * 1000)
                historyInfo['name_ws'] = hit['name']
                historyInfo['url'] = hit['url']
                historyInfo['query'] = hit['query']
                historyInfo['start_date'] = int(hit['end_date'])
                historyInfo['end_date'] = int(to_timestamp(untilDate) * 1000)
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

                try:
                    conn.execute_delete_table(config.TEMPORARY_INDEX_NAME)
                    conn.execute_create_table(config.TEMPORARY_INDEX_NAME)
                except Exception as err:
                    return "Something fail with the cleaning "

        return numProviders

if __name__ == '__main__':

    try:
        conn = DB.U_DBConnection().get_connection(config.DB_NAME)
        main(conn)
    except EH.DBConnectionError as err:
        LH.fileLogger.error("The connection to the database failed, exit")
        exit
    finally:
        DB.U_DBConnection().del_connection()
