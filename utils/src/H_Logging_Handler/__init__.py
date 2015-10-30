'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
import logging
from logging.config import fileConfig

logging.config.fileConfig('../logging_config.ini')
fileLogger = logging.getLogger('file_logger')
consoleLogger = logging.getLogger('root')

# Remove external logs from stdout and stderr. Can we forward these to our logs?

elasticSearchLogger = logging.getLogger('elasticsearch')
elasticSearchLogger.propagate = False
elasticSearchTraceLogger = logging.getLogger('elasticsearch.trace')
elasticSearchTraceLogger.propagate = False
urlibLogger = logging.getLogger('urllib3.util.retry')
urlibLogger.propagate = False
urlib2Logger = logging.getLogger('urllib3.connectionpool')
urlib2Logger.propagate = False
