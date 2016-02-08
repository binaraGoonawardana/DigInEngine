__author__ = 'Marlon Abeykoon'
# -*- coding: utf-8 -*-
import tweepy
import sys
import pika
import json
import logging
import time
import traceback
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('social_media_analytrics.log')
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  SocialMediaService  ---------------------------------------------')
logger.info('Starting log')

consumer_key = "xHl7DEIJjH8pNM2kn8Q9EddGy"
consumer_secret = "eVxjTk7d4Z41VQ2Kt7kcOF6aFjTQqqiWIKgM8xhqkMYoE8Pxmq"
access_token = "79675949-r2z1UIBa5eeiIQBO6e4PSL9ytCMpfPUHC2lNoI7o2"
access_token_secret = "dBH5sLkief3oz7sftVwP30at1fij9dFm4hL02tpCUFxbj"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def start_listner(unique_id, keyword, limit=200):

    class CustomStreamListener(tweepy.StreamListener):
        def __init__(self, api):
            logger.info('runnning')
            self.api = api
            super(tweepy.StreamListener, self).__init__()

            #setup rabbitMQ Connection
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost')
            )
            self.channel = connection.channel()

            #set max queue size
            args = {"x-max-length": 200}

            self.channel.queue_declare(queue=unique_id, arguments=args)

        def on_status(self, status):
            #print status.text.encode('utf-8'), "\n"

            data = {}
            data['text'] = status.text.encode('utf-8')
            data['created_at'] = time.mktime(status.created_at.timetuple())
            data['geo'] = status.geo
            data['source'] = status.source
            logger.info(data)

            #queue the tweet
            self.channel.basic_publish(exchange='',
                                        routing_key=unique_id,
                                        body=json.dumps(data))

        def on_error(self, status_code):
            #print >> sys.stderr, 'Encountered error with status code:', status_code
            return True  # Don't kill the stream

        def on_timeout(self):
            #print >> sys.stderr, 'Timeout...'
            return True  # Don't kill the stream

    #f = open("Output.txt","r")
    #keyword = f.read()
    #logger.info(keyword)
    #f.close()
    sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
    try:
        logger.info('tracking started')
        logger.info(keyword)
        kw = keyword
        sapi.filter(track=[kw])
    except Exception, err:
        logger.info(keyword)
        logger.info(traceback.format_exc())

# if __name__ == "__main__":
#     logger.info("just now started")
#     # try:
#         # a = str(sys.argv[1])
#         #b = str(sys.argv[2])
#         #c = int(sys.argv[5])
#         #logger.info(a)
#     #     logger.info(b)
#     # except Exception, err:
#     #     logger.info("inside main")
#
#     start_listner()
#     # start_listner('nokia_queue', 'Nokia', limit=200)
if __name__ == "__main__":
    logger.info("just now started")
    try:
        a = str(sys.argv[1])
        b = str(sys.argv[2])
        #c = int(sys.argv[5])
        logger.info(a)
        logger.info(b)
    except Exception, err:
        logger.exception("inside main")
    else:
        start_listner(a, b)