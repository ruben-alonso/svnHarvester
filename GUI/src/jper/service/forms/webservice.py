'''
Created on 18 Nov 2015

Form for webservice

@author: Mateusz.Kasiuba
'''

from wtforms import Form, BooleanField, TextField, PasswordField, validators, SelectField
import utils.config as config
from utils.config import MULTI_PAGE, FREQUENCY_DAILY, FREQUENCY_WEEKLY,\
    FREQUENCY_MONTHLY

'''
name
url
query
email
end_date
frequency
engine
active
wait_window
'''
class WebserviceForm(Form):
    name = TextField('Name', [validators.Length(min=4, max=25), validators.Required()])
    url = TextField('Url', [validators.Length(min=6, max=35), validators.Required()])
    query = TextField('Query', [validators.Required()])
    email = TextField('Email', [validators.Email()])
    end_date = TextField('End Date', [validators.Required()])
    frequency =  SelectField('Frequency', choices=[(FREQUENCY_DAILY, 'Daily'), (FREQUENCY_WEEKLY, 'Weekly'), (FREQUENCY_MONTHLY, 'Monthly')])
    engine = SelectField('Engine', choices=[(MULTI_PAGE, 'MultiPage EPMC')])
    wait_window = TextField('Wait Window', [validators.Required(), validators.number_range(0)])
    active = BooleanField('Active')