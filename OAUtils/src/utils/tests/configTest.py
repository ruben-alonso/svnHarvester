'''
Created on 3 Nov 2015

@author: Ruben.Alonso
@Description: Config file for tests
'''
# Elastic search tests connection variables
# TODO: try an invalid address and a valid one as a list
validESConn = [{'host': 'localhost', 'port': 9200}]
invalidAddrESConn = [{'host': '10.111.13.46', 'port': 9200}]
invalidPortESConn = [{'host': '10.111.13.46', 'port': 91}]
emptyConn = [{'host': '', 'port': ''}]
validESName = 'elastic_search_v1'
invalidESName = 'fakeDB'

stringESConn = "[{'host': 'localhost', 'port': 9200}]"
stringESConnSwap = "[{'port': 9200, 'host': 'localhost'}]"

ESCorrectValue = 200

# EPMC test queries
xmlURLQuery = "http://www.ebi.ac.uk/europepmc/webservices/rest/search?query=ext_id:781840%20&format=xml"
jsonURLQuery = "http://www.ebi.ac.uk/europepmc/webservices/rest/search?query=ext_id:781840%20&format=json"

badURLQuery = "A"

xmlResult = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><responseWrapper><version>4.3</version><hitCount>1</hitCount><request><query>ext_id:781840 </query><resultType>lite</resultType><synonym>false</synonym><page>1</page><pageSize>25</pageSize></request><resultList><result><id>781840</id><source>MED</source><pmid>781840</pmid><DOI>10.1126/science.781840</DOI><title>Human malaria parasites in continuous culture.</title><authorString>Trager W, Jensen JB.</authorString><journalTitle>Science</journalTitle><issue>4254</issue><journalVolume>193</journalVolume><pubYear>1976</pubYear><journalIssn>0036-8075</journalIssn><pageInfo>673-675</pageInfo><pubType>journal article; research support, u.s. gov't, non-p.h.s.; research support, u.s. gov't, p.h.s.; in vitro</pubType><inEPMC>N</inEPMC><inPMC>N</inPMC><citedByCount>2288</citedByCount><hasReferences>N</hasReferences><hasTextMinedTerms>N</hasTextMinedTerms><hasDbCrossReferences>N</hasDbCrossReferences><hasLabsLinks>Y</hasLabsLinks><hasTMAccessionNumbers>N</hasTMAccessionNumbers><luceneScore>17257.39</luceneScore><hasBook>N</hasBook></result></resultList></responseWrapper>"

jsonResult = "{\"version\":\"4.3\",\"hitCount\":1,\"request\":{\"query\":\"ext_id:781840 \",\"resultType\":\"lite\",\"synonym\":false,\"page\":1,\"pageSize\":25},\"resultList\":{\"result\":[{\"id\":\"781840\",\"source\":\"MED\",\"pmid\":\"781840\",\"title\":\"Human malaria parasites in continuous culture.\",\"authorString\":\"Trager W, Jensen JB.\",\"journalTitle\":\"Science\",\"issue\":\"4254\",\"journalVolume\":\"193\",\"pubYear\":\"1976\",\"journalIssn\":\"0036-8075\",\"pageInfo\":\"673-675\",\"pubType\":\"journal article; research support, u.s. gov't, non-p.h.s.; research support, u.s. gov't, p.h.s.; in vitro\",\"inEPMC\":\"N\",\"inPMC\":\"N\",\"citedByCount\":2288,\"hasReferences\":\"N\",\"hasTextMinedTerms\":\"N\",\"hasDbCrossReferences\":\"N\",\"hasLabsLinks\":\"Y\",\"hasTMAccessionNumbers\":\"N\",\"luceneScore\":\"17257.39\",\"hasBook\":\"N\",\"doi\":\"10.1126/science.781840\"}]}}"