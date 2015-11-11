import os, sys, inspect
from configparser import ConfigParser
# realpath() will make your script run, even if you symlink it :)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

config = ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__))+'\\projects_paths.ini')
paths = []

for item in config['PATHS']:
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],config['PATHS'][item])))
    paths.append(cmd_subfolder)
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

from Query_Engine.QueryInvoker import H_QueryInvoker
from OA_DB_Connector.DB_Connector import H_DBConnection
import OA_Logging_Handler.Logging_Handler as LH
from config import TEMPORARY_DOCTYPE_NAME, TEMPORARY_INDEX_NAME, DB_NAME
import coverage
import unittest
    
for item in paths:
    tests = unittest.TestLoader().discover(item)
    unittest.TextTestRunner(verbosity=2).run(tests)
