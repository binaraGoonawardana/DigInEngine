__author__ = 'Marlon Abeykoon'

import modules.wordcloud_ntstreaming as wc
import socket
import pika
import json

def sendSocketMessage(message):
    """
    Send a message to a socket
    """
    print 'socket', message
    try:
        client = socket.socket(socket.AF_INET,
                               socket.SOCK_STREAM)
        client.connect(('127.0.0.1', 3333))
        client.send(message)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
    except Exception, msg:
        print msg


def process_social_media_data(social_medium, size=10):
    connection = pika.BlockingConnection()
    channel = connection.channel()
    tweets = []
    count = 0
    for method_frame, properties, body in channel.consume('twitter_topic_feed'):
        print type (body)
        tweets.append(json.loads(body)['text'])
        print 'body %s' % body
        print 'properties %s' % properties
        print 'method_frame %s' % method_frame
        count += 1

        # Acknowledge the message
        channel.basic_ack(method_frame.delivery_tag)

        # Escape out of the loop after 10 messages
        if count == size:
            break
    print 'tweets string' , tweets
    tweets_str = ', '.join(tweets)
    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    print 'Requeued %i messages' % requeued_messages
    data = wc.wordcloud_json(tweets_str)
    print data
    status = sendSocketMessage(data)
    print status
    return status



if __name__ == "__main__":
    process_social_media_data('t')
    #sendSocketMessage("Python rocks!")