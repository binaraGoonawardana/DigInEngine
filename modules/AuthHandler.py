__author__ = 'Sajeetharan'
import web
import logging
from urllib2 import Request, urlopen, URLError
import os
import sys
sys.path.append("...")
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import configs.ConfigHandler as conf
import requests
import json
logger = logging.getLogger(__name__)

def GetSession(SecurityToken, Domain):
           secToken = SecurityToken
           Domain = Domain
           AuthURL = conf.get_conf('DatasourceConfig.ini','AUTH')
           url = AuthURL['URL'] +"/GetSession/"+secToken+"/"+Domain
           try:
              #assert isinstance(url, object)
              response = requests.get(url)

           except URLError, err:
              logger.info("Authorization failed",err)
           return  response
           #return json.loads(json.dumps(response._content))


