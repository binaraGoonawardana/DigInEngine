__author__ = 'Manura Omal Bhagya'
import datetime as dt
import sys,os
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
import CacheController as cache
import logging
import  wordcloud_ntstreaming as wnc
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('wordcloudStraming.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('--------------------------------------  WordCloud_Streaming  ------------------------------------------------------')
logger.info('Starting log')

#tweet ="python python python python #python python python @python python !python python python's +python DUO software bamba"
#tweet ="python work wordcloud piri #python cold help @srilank colombo !colombo work marlon's +pet"

def insert_data(dj):
    date = dt.datetime.now()
    cache.insert_data([{'d_name': "test", 'DateTime': date, 'data' : dj}],'wordcloud')
    logger.info('Inserted results into wordcloud table')

def update_data(dj):
    date = dt.datetime.now()
    ##update all the values
    cache.update_data('wordcloud', 'WHERE d_name = "test"', data = dj, DateTime = date)
    logger.info('Inserted results into wordcloud table')

def wcloud_stream(tweet):
    try:
        logger.info('Getting data from Cache if there is')
        c_data = json.loads(cache.get_data('wordcloud','d_name,data','d_name ="test"').data)
        logger.info('calc word count for a new tweet')
        n = json.loads(wnc.wordcloud_json(tweet))

        logger.info('Agreegate previous and current tweets')
        for t in n:
            if t in c_data:
                c_data[t] = c_data[t] + n[t]
            else:
                c_data.update({t:n[t]})

        logger.info('update MemSql')
        update_data(json.dumps(c_data))

    except Exception, err:
        logger.info('Insert to MemSQL, if dash boardname is ot existing')
        c_data = json.loads(wnc.wordcloud_json(tweet))
        insert_data(json.dumps(c_data))
    logger.info('Finish and return')
    return json.dumps(c_data)





