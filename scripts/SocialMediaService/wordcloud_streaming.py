__author__ = 'Manura Omal Bhagya'
import datetime as dt
import scripts.DigINCacheEngine.CacheController as cache
import logging
import modules.wordcloud_ntstreaming as wnc
import json
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/WordcloudStreaming.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('--------------------------------------  WordCloud_Streaming  ------------------------------------------------------')
logger.info('Starting log')

tweet ="Mantan wordcloud Chelsea, wordcloud Lukaku, wordcloud ia wordcloud terluka usai melihat eks timnya mengalami kesulitan musim i\u2026 https://t.co/DZcjLQGnzH"

#tweet ="python python python python #python python python @python python !python python python's +python DUO software bamba"
#tweet ="python work wordcloud piri #python cold help @srilank colombo !colombo work marlon's +pet"

def insert_data(dj):
    date = dt.datetime.now()
    cache.insert_data([{'d_name': "test1", 'DateTime': date, 'data' : dj}],'wordcloud')
    logger.info('Inserted results into wordcloud table')

def update_data(dj):
    date = dt.datetime.now()
    ##update all the values
    cache.update_data('wordcloud', 'WHERE d_name = "test1"', data = dj, DateTime = date)
    logger.info('Inserted results into wordcloud table')

def wcloud_stream(tweet):
    try:
        logger.info('Getting data from Cache if there is')
        c_data = json.loads(cache.get_data('wordcloud','d_name,data','d_name ="test1"').data)
        logger.info('calc word count for a new tweet')
        dat = wnc.wordcloud_json(tweet)
        n = json.loads(dat)

        logger.info('Agreegate previous and current tweets')
        for t in n:
            if t in c_data:
                c_data[t] = c_data[t] + n[t]
            else:
                c_data.update({t:n[t]})

        logger.info('update MemSql')
        c_data=dict(sorted(c_data.items(), key=lambda x: x[1],reverse=True)[:250])
        update_data(json.dumps(c_data))

    except Exception, err:
        logger.info(err, 'Insert to MemSQL, if dash boardname is ot existing')
        dat = wnc.wordcloud_json(tweet)
        c_data = json.loads(dat)
        c_data=dict(sorted(c_data.items(), key=lambda x: x[1],reverse=True)[:250])
        insert_data(json.dumps(c_data))
    logger.info('Finish and return')

    return json.dumps(c_data)


#print wcloud_stram(tweet)


