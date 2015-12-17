__author__ = 'Marlon Abeykoon'
# -*- coding: utf-8 -*-
import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth
import modules.wordcloud_ntstreaming as wc
import logging
import json
import twitter
from twitter import hashtag
import tweepy
import pika

 #setup queue


def get_account_summary(auth):
    data = {'name': auth.name,
            'profile_image_url': auth.profile_image_url,
            'id': auth.id,
            'favourites_count': auth.favourites_count,
            'followers_count': auth.followers_count,
            'friends_count': auth.friends_count,
            'statuses_count': auth.statuses_count,
            'created_at': auth.created_at
            }
    return data

def hashtag_search(auth, hash_tag):

    api = tweepy.API(auth)
    search_text = hash_tag
    search_number = 2
    search_result = api.search(search_text, rpp=search_number)
    tweets_lst = []
    print search_result
    for i in search_result:
        tweets_lst.append(i.text.encode('UTF8'))

    tweets = ', '.join(tweets_lst)
    print tweets
    data = wc.wordcloud_json(tweets)
    print data
    return data


def get_streaming_tweets(id, keyword, size=10):
    connection = pika.BlockingConnection()
    channel = connection.channel()
    tweets = []
    count = 0
    for method_frame, properties, body in channel.consume(keyword):

        tweets.append(json.loads(body))
        count += 1

        # Acknowledge the message
        channel.basic_ack(method_frame.delivery_tag)

        # Escape out of the loop after 10 messages
        if count == size:
            break

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    print 'Requeued %i messages' % requeued_messages

    return tweets

    #followers_object = auth.GetFollowersCount()
    # followers = []
    # for i in range(0, len(followers_object)):
    #     print followers_object.__getitem__(i).AsDict()
    #     print type(followers_object.__getitem__(i).AsDict())
    #     followers.append(followers_object.__getitem__(i).AsDict())

    #profile_image = auth.VerifyCredentials().GetProfileImageUrl

    #favourites_object = auth.GetFavouritesCount()
    # favourites = []
    #
    # for i in range(0, len(favourites_object)):
    #     print favourites_object.__getitem__(i).AsDict()
    #     print type(favourites_object.__getitem__(i).AsDict())
    #     favourites.append(favourites_object.__getitem__(i).AsDict())

    # data = {'favourites': favourites_object,
    #         'followers': followers_object,
    #         'profile_image': 0}
    # print data
    # return json.dumps(data)