__author__ = 'Sajeetharan'

import logging
from urllib2 import URLError
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



