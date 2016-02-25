__author__ = 'Sajeetharan'
import web
import logging
from urllib2 import Request, urlopen, URLError
import configs.ConfigHandler as conf
import requests
import json
logger = logging.getLogger(__name__)

def GetSession(SecurityToken, Domain):
           secToken = SecurityToken
           Domain = Domain
           AuthURL = conf.get_conf('DatasourceConfig.ini','AUTH')
           print type(AuthURL)
           print type(secToken)
           print type(Domain)
           url = AuthURL['URL'] +"/GetSession/"+secToken+"/"+Domain
           try:
	          #assert isinstance(url, object)
              response = requests.get(url)

           except URLError, err:
              logger.info("Authorization failed",err)
           return response



