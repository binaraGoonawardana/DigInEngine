__author__ = 'Marlon'

import httplib2 as http
import json

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

def callOS(S_token, log, path):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
        'securityToken': S_token,
        'log': log
    }

    uri = 'http://45.55.83.253:3000/'


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

#if  __name__ == "__main__":
 #    callOS('tok', 'lo', 'dilshan.duoweb.info', 'twethdoorInvoice', '7')
