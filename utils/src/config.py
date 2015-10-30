'''
Created on 26 Oct 2015

@author: Ruben.Alonso
'''
configMySQL = {
               'user': 'my',
               'password': 'sql',
               'host': '127.0.0.1',
               'database': 'articles',
               'raise_on_warnings': True,
               }

configES = [{'host': '10.100.13.46', 'port': 9200}]

queryPMC = "http://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?"

queryPubMed = ""

queryEPMC = "http://www.ebi.ac.uk/europepmc/webservices/rest/profile?query="