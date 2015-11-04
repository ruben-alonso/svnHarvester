'''
Created on 26 Oct 2015

@author: Ruben.Alonso

@Description: Module created for logging information and developed for Harvester
project. It creates two different loggers, one which displays information
through the standard output (consoleLogger) and another one (fileLogger) which
saves it on a file. The setup information for the creation of these loggers is
acquired from a config file called 'logging_config.ini' It also deactivates
loggers from elasticsearch and urlib since those create a lot of noise. For
specific debug it could be useful to reactive those by changing the propagate
option to false.

'''
import logging
from logging.config import fileConfig

logging.config.fileConfig('C:/Users/Ruben.Alonso/git/RouterTest/utils/src/logging_config.ini')
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
