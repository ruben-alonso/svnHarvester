'''
Created on 21 Oct 2015

@author: Mateusz.Kasiuba
'''
from root.nested.ImportAbstract import H_ImportAbstract


class H_ImportPMC(H_ImportAbstract):
    
    def __init__(self):
        raise 'Do not use! Unfinished file'
    
    def import_data(self):
        self.bulid_url()
        return 'DATA imported from pmc'
    
    def bulid_url(self):
        H_ImportAbstract.bulid_url(self)