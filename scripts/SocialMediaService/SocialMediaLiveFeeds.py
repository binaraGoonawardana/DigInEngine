__author__ = 'Marlon Abeykoon'

import wordcloud_streaming as wc
import sys
sys.path.append("...")
import modules.sentimentAnalysis as sa
import socket
import pika
import json

def send_socket_message(message):
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


def process_social_media_data(unique_id, social_medium, size=10):

    connection = pika.BlockingConnection()
    channel = connection.channel()
    tweets = []
    count = 0
    for method_frame, properties, body in channel.consume(unique_id):
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
    data = wc.wcloud_stream(tweets_str)
    print data
    try:
        send_socket_message(data)
    except Exception, err:
        print err
        return False
    return True

def sentiment_analytical_processor(unique_id, social_medium, size=10):

    if social_medium == 'twitter':
        connection = pika.BlockingConnection()
        channel = connection.channel()
        tweets = []
        count = 0
        for method_frame, properties, body in channel.consume(unique_id):
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
        data = sa.sentiment(tweets_str)
        print data
        return data


if __name__ == "__main__":
    process_social_media_data('test','t')
    #sendSocketMessage("Python rocks!")