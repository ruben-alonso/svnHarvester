from root.nested.ImportEPMC import H_ImportEPMC
from datetime import datetime, date

x = H_ImportEPMC()
x.query = "responsible"
x.page_size = 10
date = datetime.now().date()
x.end_date = date.replace(month=10,day=28)
x.start_date = date.replace(month=10,day=20,year=2000)
x.resulttype = 'core'
x.page = '1'
x.ext_id = 26330629
data = x.import_all()
print(data)
