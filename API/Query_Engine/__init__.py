# import OA_WS_Invoker
import os
import datetime
from Query_Engine.QueryInvoker import H_QueryInvoker

print(os.getcwd())
from config import MULTI_PAGE
from datetime import date

y = H_QueryInvoker.get_engine(MULTI_PAGE, 
                   'http://www.ebi.ac.uk/europepmc/webservices/rest/search/resulttype=core&format=json&query=CREATION_DATE:[{start_date}%20TO%20{end_date}]'
                   )
y.execute()