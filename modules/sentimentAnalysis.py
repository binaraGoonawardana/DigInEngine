__author__ = 'Administrator'
# -*- coding: utf-8 -*-

from textblob import TextBlob
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
#from textblob.sentiments import NaiveBayesAnalyzer

#text = "very bad"

# determine if sentiment is positive, negative, or neutral
def sentiment(text):
    blob = TextBlob(text)
    if blob.sentiment.polarity < 0:
            sentiment = "negative"
    elif blob.sentiment.polarity == 0:
            sentiment = "neutral"
    else:
            sentiment = "positive"

    #print blob.sentiment.polarity
    #print blob.sentiment
    #print sentiment
    dict = {'polarity' : blob.sentiment.polarity, 'sentiment': sentiment}
    return json.dumps(dict)

#print sentiment("hi friend great")
""" polarity range [-1,1];
    [-1,0) = negative sentiment,
    0 = neutral,
    (0,1] = positive sentiment"""