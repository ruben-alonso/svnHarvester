'''
Created on 5 Nov 2015

@author: Ruben.Alonso
'''

import json
import OA_DB_Connector.DB_Connector as DB
import OA_Exception_Handler.Exception_Handler as EH
import OA_Logging_Handler.Logging_Handler as LH
import ImportEPMC.H_ImportEPMC as QE  # To be changed to the new engine
import config
from datetime import date, datetime
from dateutil import relativedelta


def main(conn):

    try:
        result = conn.execute_search_query(config.WEBSERVICES_INDEX_NAME,
                                           config.WEBSERVICES_DOCTYPE_NAME,
                                           "I want the active queries")
    except Exception:
        LH.fileLogger.info("The query to the DB failed, exit")
        return "it has failed because..."

    hits = result['hits']['hits']
    if not hits:
        LH.fileLogger.info('No matches found, exit')
        return "No queries, bye bye"
    else:
        for hit in hits:
            lastDate = datetime.strptime(hit['last_date'], '%Y-%m-%d').date()
            today = date.today()
            frequency = hit['frequency']
            myUrl = hit['url']
            query = hit['query']
            provider = hit['provider']
            docId = hit['docId']

            if 'daily' == frequency:
                if today == lastDate:
                    LH.fileLogger.info("Last time checked was today, exit")
                    continue
            else:
                if 'weekly' == frequency:
                    passWeekDate = today + relativedelta(days=-7)
                    if lastDate < passWeekDate:
                        LH.fileLogger.info("Last time checked was less than a week before, exit")
                        continue
                else:
                    if 'monthly' == frequency:
                        passMonthDate = today + relativedelta(months=-1)
                        if lastDate < passMonthDate:
                            LH.fileLogger.info("Last time checked was less than a month before, exit")
                            continue
                    else:
                        raise EH.InputError("Incorrect value error", "The value\
                         frequency retrieved from the DB is incorrect, please \
                         fix it to continue")

            queryEngine = QE.query_engine(provider)
            numFilesReceived = queryEngine.import_all(myUrl, today, lastDate)  #I am sending the today date just in case so we use the same for everything
            try:
                result = conn.execute_search_query(config.TEMPORARY_INDEX_NAME,
                                                   config.TEMPORARY_DOCTYPE_NAME,
                                                   query)
                print(result)
            except Exception as err:
                LH.fileLogger.info("Error retrieving documents with the chosen query")
                return "it has failed because..."

            numFilesToSend = result['hits']
            resultRouter = router_API.send(result)  #When it exist

            historyInfo = {}
            historyInfo['date'] = today
            historyInfo['name_ws'] = provider
            historyInfo['url'] = myUrl
            historyInfo['query'] = query
            historyInfo['num_files_received'] = numFilesReceived
            historyInfo['num_files_sent'] = numFilesToSend
            historyInfo['error'] = resultRouter

            json_history = json.dumps(historyInfo)

            try:
                result = conn.execute_insert_query(config.HISTORY_INDEX_NAME,
                                                   config.HISTORY_DOCTYPE_NAME,
                                                   json_history)
                LH.fileLogger("Historic entry added to the DB")
            except EH.Error as err:
                LH.fileLogger("Historic entry failed to be added to the DB",
                              err.message)
                return "has failed bacuse..."

            hit['lastDate'] = str(today.isoformat)
            result = conn.execute_update_query(config.WEBSERVICES_INDEX_NAME,
                                               config.WEBSERVICES_DOCTYPE_NAME,
                                               docId,
                                               "new date")


if __name__ == '__main__':

    try:
        conn = DB.H_DBConnection().get_connection(config.DB_NAME)  # This name will be retrieved from a config file??
        main(conn)
    except EH.DBConnectionError as err:
        LH.fileLogger.info("The connection to the database failed, exit")
        exit
    finally:
        DB.H_DBConnection().del_connection()