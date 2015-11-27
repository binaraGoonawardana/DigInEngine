__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth

def get_overview(token, insight_nodes, since=None, until=None):
    """
    :param token: access_token for the page
    :param insight_nodes: Any node mentioned in https://developers.facebook.com/docs/graph-api/reference/v2.5/insights#page_users
    :param since: From date [Optional]
    :param until: To Date [Optional]
    :return: json
    """
    if insight_nodes is None:
        metrics = ["page_stories", "page_fan_adds"]
    else:
        metrics = insight_nodes
    page_auth = SMAuth.set_token(token)
    if since is None or until is None:
        view_count = page_auth.request('me/insights/page_views')['data'][0]['values']
    else:
        view_count = page_auth.request('me/insights/page_views',
                                       args={'since': since,
                                             'until': until}
                                       )['data'][0]['values']
    print view_count
    # http://stackoverflow.com/questions/3199171/append-multiple-values-for-one-key-in-python-dictionary
    # http://stackoverflow.com/questions/5378231/python-list-to-dictionary-multiple-values-per-key

    def dictionary_builder(ori_dict, new_dict, name):
        for line in new_dict:
            if line['end_time'] in ori_dict:
                # append the new number to the existing array at this slot
                print line['end_time']
                nested_dict = {name: line['value']}
                ori_dict[line['end_time']].append(nested_dict)
            else:
                # create a new array in this slot
                ori_dict[line['end_time']] = {name: line['value']}
                print ori_dict

        return ori_dict

    dict = {}

    for _ in range(0, len(view_count)):
        dict[view_count[_]["end_time"]] = [{"Likes": view_count[_]["value"]}]
        print 'ori_dict', dict

    for i in metrics:
        if since is None or until is None :
            request_result = page_auth.request('me/insights/%s' % i)['data'][0]['values']
        else:
            request_result = page_auth.request('me/insights/%s' % i,
                                               args={'since': since,
                                                     'until': until}
                                               )['data'][0]['values']
        print request_result
        dict = dictionary_builder(dict,request_result,i)

    print dict

    return dict



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




