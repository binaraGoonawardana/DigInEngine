__author__ = 'Marlon'

import httplib2 as http
import json
import urllib

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
uri = 'http://45.55.83.253:3000/'
def get_data(S_token, log, path):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
        'securityToken': S_token,
        'log': log
    }


    target = urlparse(uri+path)
    print target
    method = 'GET'
    body = ''

    h = http.Http()

    response, content = h.request(
        target.geturl(),
        method,
        body,
        headers)

    data = json.dumps(json.loads(content))
    print data
    return data

def get_fields(S_token, log,indexname):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
        'securityToken': S_token,
        'log': log
    }

    target = urlparse(uri+indexname)
    print target.geturl()
    print target
    method = 'POST'
    body = json.dumps({"Special":{"Type":"getFields", "Parameters":""}})
    h = http.Http()

    response, content = h.request(
            target.geturl(),
        method = method,
        headers =  headers,
        body=body
        )
    data = json.dumps(content)
    print data
    return data

def get_fields_with_types(S_token,log, indexname):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
        'securityToken': S_token,
        'log': log
    }

    target = urlparse(uri+indexname)
    method = 'POST'
    #body = json.dumps({"Query":{"Type":"Query", "Parameters":"select column_name, data_type from information_schema.columns where table_name = 'DemoHNB_claim'"}})
    body = json.dumps({"Query":{"Type":"Query", "Parameters":"select * from DemoHNB_claim limit 100"}})
    h = http.Http()
    response, content = h.request(
            target.geturl(),
        method = method,
        headers =  headers,
        body=body
        )
    data = json.dumps(content)
    print data
    return data
#if  __name__ == "__main__":
 #    callOS('tok', 'lo', 'dilshan.duoweb.info', 'twethdoorInvoice', '7')
