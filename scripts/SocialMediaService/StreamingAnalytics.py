__author__ = 'Marlon Abeykoon'
# -*- coding: utf-8 -*-

import tweepy
import sys
import pika
import json
import time

consumer_key = "xHl7DEIJjH8pNM2kn8Q9Eddy"
consumer_secret = "eVxjTk7d4Z41VQ2Kt7kcOF6aFjTQqqiWIKgM8xhqkMYoE8Pxm"
access_token = "79675949-r2z1UIBa5eeiIQBO6e4PSL9ytCMpfPUHC2lNoI7o"
access_token_secret = "dBH5sLkief3oz7sftVwP30at1fij9dFm4hL02tpCUFxb"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        #setup rabbitMQ Connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.channel = connection.channel()

        #set max queue size
        args = {"x-max-length": 2000}

        self.channel.queue_declare(queue='twitter_topic_feed', arguments=args)

    def on_status(self, status):
        print status.text.encode('utf-8'), "\n"

        data = {}
        data['text'] = status.text.encode('utf-8')
        data['created_at'] = time.mktime(status.created_at.timetuple())
        data['geo'] = status.geo
        data['source'] = status.source

        #queue the tweet
        self.channel.basic_publish(exchange='',
                                    routing_key='twitter_topic_feed',
                                    body=json.dumps(data))

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True  # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True  # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
# my keyword today is chelsea as the team just had a big win
sapi.filter(track=['chelsea'])