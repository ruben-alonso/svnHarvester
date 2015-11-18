'''
Created on 18 Nov 2015

Webpage - Graphic User Interface for an harvester 

@author: Mateusz.Kasiuba
'''
import uuid, json, time, requests

from service.models.harvester import HarvesterModel

from flask import Blueprint, request, url_for, flash, redirect, make_response
from flask import render_template, abort
from flask.ext.login import login_user, logout_user, current_user

harvester = Blueprint('harvester', __name__)
harvesterModel = HarvesterModel()

#This is part of their code in my opinion we should move this part of code in common class
@harvester.before_request
def restrict():
    if current_user.is_anonymous():
        if not request.path.endswith('login'):
            return redirect(request.path.rsplit('/',1)[0] + '/login')
# end part of their code should be move in common class

@harvester.route('/webservice')
def webservice():
    '''
    Page with list of webservices installed in ES database
    '''
    if not current_user.is_super:
        abort(401)
    webservice = harvesterModel.get_webservices()
    return render_template('harvester/webservice.html', webservice_list = webservice)


@harvester.route('/history')
def history():
    '''
    Page with list history quesries form harvester
    '''
    if not current_user.is_super:
        abort(401)
    history = harvesterModel.get_history()
    return render_template('harvester/webservice.html', history_list = history)

@harvester.route('/details/<webservice_id>', methods=['GET','POST'])
def details(webservice_id):
    '''
    Page with details 
    '''
    pass