__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth

def insight_metric(token, insight_node, since=None, until=None):
    """
    :param token: access_token for the page
    :param insight_node: Any node mentioned in https://developers.facebook.com/docs/graph-api/reference/v2.5/insights#page_users
    :param since: From date [Optional]
    :param until: To Date [Optional]
    :return: json
    """
    page_auth = SMAuth.set_token(token)
    metric_name = insight_node
    #data = json.dumps(page_auth.get_connections("me", "insights/{0}?since={1}&until={2}".format(metric_name,since,until)))
    #data = json.dumps(page_auth.get_connections("me", "insights/{0}".format(metric_name)))
    if since is None or until is None :
        data = page_auth.request('me/insights/{0}'.format(metric_name))['data']
    else:
        data = page_auth.request('me/insights/{0}'.format(metric_name),
                                 args={'since': since,
                                       'until': until})['data']

    return data

def get_promotional_info(token, promotion_node):
    page_auth = SMAuth.set_token(token)
    #data = page_auth.request('me/comments')['data']
    data = page_auth.get_connections("me", "Comments")
    return data







    #page_user_demographics('CAACEdEose0cBANi6gbZB7y0nFASqzZA56NvbQFalBKZAoGfM4UcSRbHSvPZCq4ZCPu2KTKA6uRQYE2wY0LFKJi0w3MVGHJ2ZBXz5rD3JcD9dlNBCWQX5VS6WBbnCPjobZAAtAT00ZCgZBzS2O2LhDrPQPe0fsbAjqybA24BZByinoPR2hDdO8nUK9IITrHAJd2DfYZD', 'page_fans_city')

    # page_fans_gender_age
    # titles = graph.get_connections("me", "insights/page_fans_gender_age")
    # titles1 = [title['title'] for title in titles['data']]
    # print json.dumps(titles)
# friends = graph.get_connections("me", "friends")

#
# friends = graph.get_connections("me", "likes?summary=true")
# print friends

# friend_list = [friend['name'] for friend in friends['data']]
# pages = graph.get_connections(id='me', connection_name='Page')
# print pages
# profile = graph.get_object("me/likes/total_count?summary=true")
# print json.dumps(profile)




