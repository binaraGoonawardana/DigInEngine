__author__ = 'Marlon Abeykoon'

import facebook
import FacebookAnalytics as fb
import json
import web

urls = (
    '/pageinsightmetric(.*)', 'FBInsightMetric',
    '/promtionalinfo(.*)', 'FBPromotionalInfo'
)

app = web.application(urls, globals())

#http://localhost:8080/pageinsightmetric?metric_name=page_views&token=CAACEdEose0cBAKbXjbPZBP7npZCe09abf1zgp5lagXNOj5x2WTbgZAq8MalHdA6hZCGbUCkl1jZAPoSeQetKKDMSK8SlOPZCLGys8Mg9bbs0zyVJgtxY1ZA5UWxXcxjoh1zTrlReuaAT25h1ZAMDSxS16IZAdJa2Ps2nPN05ZBU2QyFx1toZB1aFfuPMnkoTUsAoQZD&since=1447509660&until=1448028060
class FBInsightMetric(web.storage):
    def GET(self,r):


        token = web.input().token
        metric_name = web.input().metric_name
        since = None
        until = None
        try:
            since = web.input().since
            until = web.input().until
        except AttributeError:
            pass
        data = json.dumps(fb.insight_metric(token, metric_name, since, until))
        print data
        return data

class FBPromotionalInfo(web.storage):
    def GET(self, r):
        token = web.input().token
        promotional_name = web.input().metric_name
        data = json.dumps(fb.get_promotional_info(token,promotional_name))
        print data
        return data



if __name__ == "__main__":
    app.run()