'''
Created on 5 Nov 2015

@author: Ruben.Alonso
'''
from __future__ import division
import json
import requests
import utils.connector.connector as DB
import utils.exception.handler as EH
import utils.logger.handler as LH
import engine.query.QueryInvoker as QE
import utils.config as config
from datetime import datetime
from dateutil.relativedelta import relativedelta
from service import models
from octopus.lib import dataobj


def to_timestamp(dt, epoch=datetime(1970, 1, 1)):
    """Function to convert datetime into timestamp since it doesn't exist in 
    python v2

    :param dt: date to be converted
    :param epoch: date to start counting from to create the date stamp

    :return timestamp of the date since epoch
    """
    td = dt - epoch
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6


def match_router_json(document):
    """Function which converts DB json format into the notification structure
    for Router.

    :param document: the json document to be converted.

    :return document in the IncomingNotification Router structure
    """
    notification = models.IncomingNotification()
    # Add event as acceptance to the incoming notification
    notification._set_single('event',
                             'acceptance',
                             coerce=dataobj.to_unicode())
    # Add links to the incoming notification
#     uc = dataobj.to_unicode()
#     if 'fullTextUrlList'in document:
#         for link in document['fullTextUrlList']['fullTextUrl']:
#             obj = {"url": notification._coerce(link['url'], uc),
#                    "type": notification._coerce('fulltext', uc),
#                    "format": notification._coerce(link['documentStyle'], uc)}
#             notification._delete_from_list("links", matchsub=obj, prune=False)
#             notification._add_to_list("links", obj)
    # Add provider information to incoming notification
    notification._set_single("provider.agent",
                             'EPMC',
                             coerce=dataobj.to_unicode())
    notification._set_single("provider.ref",
                             document['id'],
                             coerce=dataobj.to_unicode())
    # Add content.packaging_format to incoming notification
    notification._set_single("content.packaging_format",
                             'application/json',
                             coerce=dataobj.to_unicode())
    # Add embargo end date to incoming notification
    if 'embargoDate' in document:
        notification._set_single("embargo.end",
                                 document['embargo'],
                                 coerce=dataobj.to_unicode())
    # Add document metadata using existing setters
    if 'title' in document:
        notification.title = document['title']
    if 'journalInfo' in document:
        if 'title' in document['journalInfo']['journal']:
            notification.source_name = document['journalInfo']['journal']['title']
        if 'issn' in document['journalInfo']['journal']:
            notification.add_source_identifier("issn", document['journalInfo']['journal']['issn'])
        if 'essn' in document['journalInfo']['journal']:
            notification.add_source_identifier("eissn", document['journalInfo']['journal']['essn'])
    if 'doi' in document:
        notification.add_identifier("doi", document['doi'])
    if 'pmid' in document:
        notification.add_identifier("pmid", document['pmid'])
    if 'pmcid' in document:
        notification.add_identifier("pmcid", document['pmcid'])
    if 'firstPublicationDate' in document:
        notification.publication_date = document['firstPublicationDate']
    #notification.date_accepted = document['']  # TODO: which date?
    #notification.date_submitted = document['']  # TODO: which date?
    if 'pubTypeList' in document:
        pubType = ""
        for publication in document['pubTypeList']['pubType']:
            pubType = (publication if pubType == "" else publication + "; " + pubType)
        notification.type = pubType
    if 'language' in document:
        notification.language = document['language']
    if 'authorList' in document:
        for author in document['authorList']['author']:
            auxAuthor = {}
            auxAuthor['name'] = author['fullName']
            if 'identifier' in author:
                auxAuthor['identifier'] = author['identifier']
            auxAuthor['affiliation'] = author['affiliation']
            notification.add_author(auxAuthor)
    if 'grantsList' in document:
        for grant in document['grantsList']['grant']:
            auxGrant = {}
            auxGrant['name'] = grant['agency']
            auxGrant['grant_number'] = grant['grantId']
            notification.add_project(auxGrant)
    if 'keywordList' in document:
        for keyWord in document['keywordList']:
            notification.add_subject(keyWord)
    if 'meshHeadingList' in document:
        for keyWord in document['meshHeadingList']:
            notification.add_subject(keyWord['descriptionName'])
    return notification.json()


def send_to_router(documents):
    """Function which receives the documents with affiliation and process them
    converting those into Router format and send them.

    :param: documents: list retrieved from DB with filter

    :return: number of documents send to router
    """
    url = "http://127.0.0.1:5998/api/v1/validation"
    url += '?api_key=' + 'f78d0001-b6ab-4989-95b8-c46dcba2168e'  # My user's ID
    i = 0
    try:
        for doc in documents['hits']:
            notification = match_router_json(doc['_source'])
            print(notification)
            resp = requests.post(url, data=notification, headers={"Content-Type" : "application/json"})
            if 202 == resp.status_code:
                LH.fileLogger.info("Article {art} accepted. {resp}".format(art=doc['_source']['id'], resp=resp.content))
            elif 400 == resp.status_code:
                LH.fileLogger.error("Article {art}. Bad request {resp}".format(art=doc['_source']['id'], resp=resp.content))
            elif 401 == resp.status_code:
                LH.fileLogger.error("Article {art}. Authentication failure".format(art=doc['_source']['id']))
            else:
                LH.fileLogger.error("Article {art}. Some other error {resp}".format(art=doc['_source']['id'], resp=resp.content))
            i += 1
    except Exception as err:
        print("Loop " + str(err))
    return i


def process_data(conn, hit, docId, lastDate, untilDate):
    """ Function to process the data with the information received from
    elasticsearch DB.

    :param: conn: DB connection
    :param: hit: WS provider entry to process
    :param: today: today date
    :param: lastDate: last time the query was executed

    :return: number of files received from WS provider, number of files which
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

    # TODO: Add error checking when we know who to call router
    numFilesToSend = send_to_router(result['hits'])

    print("Received from " + str(provider) + " " + str(numFilesReceived) + " files")
    print("Sent to router " + str(numFilesToSend) + " files")

    todayTimeStamp = {
                         'doc': {'end_date': int(to_timestamp(untilDate) * 1000)}
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
