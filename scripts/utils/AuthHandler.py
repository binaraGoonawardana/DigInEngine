__author__ = 'Sajeetharan'
__version__ = '1.0.0.1'

import logging
from urllib2 import URLError
import json
import ast
import configs.ConfigHandler as conf
import requests
logger = logging.getLogger(__name__)

def GetSession(SecurityToken):
           secToken = SecurityToken
           AuthURL = conf.get_conf('DatasourceConfig.ini','AUTH')
           url = AuthURL['URL'] +"/GetSession/"+secToken+"/"+'Nil'
           try:
              response = requests.get(url)
           except URLError, err:
              print err
              logger.error("Authorization failed")
              logger.error(err)
              response = None
           return response

def get_security_level(security_token):
    secToken = security_token
    tenant = json.loads(GetSession(secToken).text)['Domain']
    AuthURL = conf.get_conf('DatasourceConfig.ini', 'AUTH')
    url = AuthURL['URL'] + "/tenant/Autherized/" + tenant
    try:
        response = requests.get(url,headers={"Securitytoken":security_token})
        if json.loads(response.text)['Autherized']:
           security_level = json.loads(response.text)['SecurityLevel']
           return security_level
    except Exception, err:
        print err
        raise

def get_group_users(tenant, group_id):

    url = "http://"+ tenant + "/apis/usercommon/getUserFromGroup/" + group_id
    try:
        response = json.loads(requests.get(url).text)
        if type(response) is list:
            return response
        if not response['success']:
            raise
    except Exception, err:
        print err
        raise

def send_email(security_token, **kwargs):

    AuthURL = conf.get_conf('DatasourceConfig.ini', 'AUTH')
    mail_data = {
        "type": "email",
        "to": kwargs.get('to_addresses'),
        "cc": kwargs.get('cc_addresses'),
        "subject": kwargs.get('subject'),
        "from": kwargs.get('from'),
        "Namespace": "com.duosoftware.com",
        "TemplateID": kwargs.get('template_id'),
        "DefaultParams": kwargs.get('default_params'),
        "CustomParams": kwargs.get('custom_params')
    }
    url = '{0}/command/notification'.format(AuthURL['CEB'])
    data_to_send = json.dumps(mail_data)

    headers = {'Content-type': 'application/json',
               'SecurityToken': security_token}
    try:
        response = requests.post(url, data=data_to_send, headers=headers)
        print response
    except Exception, err:
        print err
        raise