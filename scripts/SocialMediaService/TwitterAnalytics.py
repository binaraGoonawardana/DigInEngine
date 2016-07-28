__author__ = 'Marlon Abeykoon'
# -*- coding: utf-8 -*-
import sys
sys.path.append("...")
import json

import pika

 #setup queue


def get_account_summary(api, id_list):
    data = api.lookup_users(user_ids=id_list)
    users = []
    for user in data:
        data = {'name': user.name,
                'profile_image_url': user.profile_image_url,
                'id': user.id,
                'favourites_count': user.favourites_count,
                'followers_count': user.followers_count,
                'friends_count': user.friends_count,
                'statuses_count': user.statuses_count,
                }
        users.append(data)
        print users
    return users

def hashtag_search(api, hash_tag):

    search_text = hash_tag
    search_number = 2
    search_result = api.search(search_text, rpp=search_number)
    tweets_lst = []
    for i in search_result:
        tweets_lst.append(i.text.encode('UTF8'))

    tweets = ', '.join(tweets_lst)
    return tweets


def get_streaming_tweets(id, keyword, size=10):
    connection = pika.BlockingConnection()
    channel = connection.channel()
    tweets = []
    count = 0
    for method_frame, properties, body in channel.consume(keyword):
        print properties
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