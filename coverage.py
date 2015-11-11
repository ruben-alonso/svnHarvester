import os, sys, inspect
# realpath() will make your script run, even if you symlink it :)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)
 # use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"API")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)
    
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"OAUtils\\src")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"API\\Query_Engine\\tests")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

from Query_Engine.QueryInvoker import H_QueryInvoker
from OA_DB_Connector.DB_Connector import H_DBConnection
import OA_Logging_Handler.Logging_Handler as LH
from config import TEMPORARY_DOCTYPE_NAME, TEMPORARY_INDEX_NAME, DB_NAME
import coverage
import unittest

tests = unittest.TestLoader().discover(cmd_subfolder)
unittest.TextTestRunner(verbosity=2).run(tests)
