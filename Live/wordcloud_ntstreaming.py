__author__ = 'Manura Omal Bhagya'

import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
import json
import logging

#tweet ="python work wordcloud piri #python cold help @srilank colombo !colombo work marlon's +pet"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('wordcloud.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  WordCloud_withoutStreaming  ------------------------------------------------------')
logger.info('Starting log')

# print tweet

def wordcloud_json(tweet):
    cv = CountVectorizer(min_df=0, stop_words="english", max_features=200,strip_accents="ascii", decode_error ="ignore")

    counts = cv.fit_transform([tweet]).toarray().ravel()
    words = np.array(cv.get_feature_names())

    counts = map(int, counts)

    words = [i.tolist() for i in words]
    words = [x.encode('UTF8') for x in words]

    dictionary = dict(zip(words, counts))

    dj = json.dumps(dictionary)
    return dj
#print wordcloud_json(tweet)
logger.info('Stopped log')


# def wordcloud_json(tweet):
#     logger.info('Received  tweets')
#     tweet = tweet.split()
#     logger.debug('word cloud received tweets splitted: %s',tweet)
#     #print tweet
#
#     unique,pos = np.unique(tweet,return_inverse=True)
#     counts = np.bincount(pos)
#     maxsort = counts.argsort()[::-1]
#
#     logger.info('Processed  tweets')
#     words =  unique[maxsort]
#     counts = counts[maxsort]
#     logger.info('Get Counts')
#     logger.debug('words: %s and counts:%s',words,counts)
#    #print words, counts
#
#     dictionary = dict(zip(words, counts))
#     logger.info('Dictionary Created')
#     dj = json.dumps(dictionary)
#     logger.info('Dump the dictionary to json')
#     logger.debug('Return Json:%s',dj)
#     #print dictionary


#
#     #print dj
#     return dj
#
# logger.info('Stopped log')
#
#

