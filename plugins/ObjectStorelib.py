__author__ = 'Marlon'

import httplib
import json

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=UTF-8',
    'securityToken': 'tt',
    'log': 'll'
}

