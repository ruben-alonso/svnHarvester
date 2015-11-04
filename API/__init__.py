from ImportEPMC import H_ImportEPMC
from datetime import datetime, date
from tests.TestImportEPMC import TestImportEPMC

# x = H_ImportEPMC()
# x.query = "responsible"
# x.page_size = 500
# date = datetime.now().date()
# # x.end_date = date.replace(month=10,day=28)
# # x.start_date = date.replace(month=10,day=20,year=2000)
# x.resulttype = 'core'
# x.page = '1'
# # x.ext_id = 26330629
# try:
#     data = x.import_all()
#     print(data)
# except :
#     print("Oops! Something goes wrong!")
#     print(x.api_query)

test = TestImportEPMC()
test.test_date_setter()
test.test_date_setter_wrong()
test.test_ext_id_setter()
test.test_format_setter()
test.test_format_setter_wrong()
test.test_new_year_import()
test.test_page_size_setter_wrong()
test.test_page_setter_wrong()
test.test_page_setter()
test.test_page_size_setter()
test.test_resulttype_setter()
test.test_resulttype_setter_wrong()
test.test_query_setter()
test.test_simple_test_import_type_result()
test.test_import_by_query_and_date_and_ext_id()
test.test_unicode_encode_data()
test.test_import_data_as_xml()
test.test_import_by_query_and_date_and_ext_id()
test.test_import_data_as_dc()
test.test_import_resulttype_as_idlist()
test.test_import_resulttype_as_lite()
test.test_wrong_amount_in_import_all_data()