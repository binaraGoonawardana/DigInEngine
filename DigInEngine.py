__author__ = 'Marlon Abeykoon'
__version__ =  'v3.0.0.2'

import sys,os
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir


import web
from time import strftime
import logging
from multiprocessing import Process
import configs.ConfigHandler as conf
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as pg
import modules.BigQueryHandler as bq
import modules.AuthHandler as Auth
import modules.CommonMessageGenerator as comm
import scripts
print 'Loading configs...'
datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']

print 'Checking configs...'

print 'Checking datasource connections...'
try:
    mssql.execute_query('SELECT 1')
    print "MSSQL connection successful!"
except:
    print "Error connecting to MSSQL server!"
    pass
try:
    pg.execute_query('SELECT 1')
    print "PostgreSQL connection successful!"
except:
    print "Error connecting to pgSQL server!"
    pass
try:
    bq.execute_query('SELECT 1')
    print "BigQuery connection successful!"
except:
    print "Error connecting to BigQuery server!"
    pass
print 'Cache initializing...'
# try:
#     p = Process(target=scripts.DigINCacheEngine.CacheGarbageCleaner.initiate_cleaner)
#     p.start()
# except:
#     print "Error initializing cache"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('DigInEngine.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


print(
"""

================================================================================
  _____    _           _____             ______                   _
 |  __ \  (_)         |_   _|           |  ____|                 (_)
 | |  | |  _    __ _    | |    _ __     | |__     _ __     __ _   _   _ __     ___
 | |  | | | |  / _` |   | |   | '_ \    |  __|   | '_ \   / _` | | | | '_ \   / _ \
 | |__| | | | | (_| |  _| |_  | | | |   | |____  | | | | | (_| | | | | | | | |  __/
 |_____/  |_|  \__, | |_____| |_| |_|   |______| |_| |_|  \__, | |_| |_| |_|  \___|
                __/ |                                      __/ |
               |___/                                      |___/
================================================================================

""")

urls = (
    '/hierarchicalsummary(.*)', 'CreateHierarchicalSummary',
    '/gethighestlevel(.*)', 'GetHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/forecast(.*)', 'Forecasting',
    '/pageoverview(.*)', 'FBOverview',
    '/demographicsinfo(.*)', 'FBPageUserLocations',
    '/fbpostswithsummary(.*)', 'FBPostsWithSummary',
    '/promtionalinfo(.*)', 'FBPromotionalInfo',
    '/twitteraccinfo(.*)', 'TwitterAccInfo',
    '/hashtag(.*)', 'BuildWordCloud',
    '/buildwordcloudrt(.*)', 'BuildWordCloudRT',
    '/streamingtweets(.*)', 'StreamingTweets',
    '/sentimentanalysis(.*)', 'SentimentAnalysis',
    '/buildwordcloudFB(.*)', 'BuildWordCloudFB',
    '/buildbipartite(.*)', 'BuildBiPartite',
    '/linear_regression(.*)', 'LinearRegression',
    '/file_upload(.*)', 'Upload',
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration',
    '/generatebubble(.*)', 'bubblechart',
    '/executeQuery(.*)', 'ExecuteQuery',
    '/createDataset(.*)', 'CreateDataset',
    '/GetFields(.*)', 'GetFields',
    '/GetTables(.*)', 'GetTables',
    '/getLayout(.*)', 'GetLayout',
    '/getQueries(.*)','GetQueries',
    '/getreportnames(.*)','GetReportNames',
    '/executeKTR(.*)','ExecuteKTR'
)


#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]&conditions=date%20=%20%272015-05-04%27%20and%20name=%27marlon%27&id=1
class CreateHierarchicalSummary(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received createHierarchicalSummary: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.LogicImplementer.LogicImplementer.create_hierarchical_summary(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed createHierarchicalSummary'
        return result

#http://localhost:8080/gethighestlevel?tablename=[digin_hnb.hnb_claims]&id=1&levels=['vehicle_usage','vehicle_class','vehicle_type']&plvl=All
class GetHighestLevel(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_highest_level: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.LogicImplementer.LogicImplementer.get_highest_level(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_highest_level'
        return result

# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by=%{27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]&id=15&t=30000
# http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]&id=15&t=30000
# localhost:8080/aggregatefields?group_by={'a1':1,'b1':2,'c1':3}&order_by={'a2':1,'b2':2,'c2':3}&agg=[[%27field1%27,%27sum%27],[%27field2%27,%27avg%27]]&tablenames={1 : 'table1', 2:'table2', 3: 'table3'}&cons=a1=2&joins={1 : 'left outer join', 2 : 'inner join'}&join_keys={1: 'ON table1.field1' , 2: 'ON table2.field2'}&db=MSSQL&id=15&t=30000
# for Single table:
# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=[[%27field1%27,%27sum%27],[%27field2%27,%27avg%27]]&tablenames={1%20:%20%27table1%27,%202:%27table2%27,%203:%20%27table3%27}&cons=a1=2&db=MSSQL&id=15&t=30000
# http://localhost:8080/aggregatefields?group_by={%27Category%27:1}&order_by={}&agg=[[%27Invoice_amount%27,%27sum%27]]&tablenames={1:%27Demo.epsi_sales%27}&cons=&db=BigQuery&id=15&t=30000&SecurityToken=0b4fac3276c5328db15e538590665d6a&Domain=duosoftware.com

class AggregateFields():

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received aggregate_fields: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.AggregationEnhancer.AggregationEnhancer.aggregate_fields(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed aggregate_fields'
        return result

#http://localhost:8080/forecast?model=Additive&pred_error_level=0.0001&alpha=0&beta=53&gamma=34&fcast_days=30&table_name=[Demo.forcast_superstoresales]&field_name_d=Date&field_name_f=Sales&steps_pday=1&m=7&interval=Daily
class Forecasting():

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received Forecasting: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.PredictiveAnalysisEngine.PredictiveAnalysisEngine.Forecasting(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed Forecasting'
        return result

# http://localhost:8080/pageoverview?metric_names=['page_views']&token=CAACEdEosecBAMs8o7vZCgwsufVOQcLynVtFzCq6Ii1LwMyMRFgcV5xFPzUWGMKfJBJZBGb33yDciESrnThNY4mAV2fn14cGEjSUZAIvx0jMt4g6M3lEO8arfNPZBDISA49vO9F7LsKQwyePkWJBSN8NuMvaIWGzTfOrkpQzItLTlSSweUX8LOZB4TQRi8p8ZD&since=1447509660&until=1448028060
class FBOverview():

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_overview: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_overview(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_overview'
        return result

class FBPageUserLocations(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_page_user_locations: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_page_user_locations(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_page_user_locations'
        return result

#http://localhost:8080/fbpostswithsummary?token=CAACEdEose0cBAJ6yiM46CxqzY3UyaTiSO2hwj451gctPwPULXdTQo8hmlufxjcKjTKySKQchoiRvBmodWivQ97tqTOmsfcZAt4b0ROwZCbFsrZAb3ED0sq3e4GbxL6OdjyAE26H9qBYs05msMm1uPdKRw2lLLfurThOKgTxdJwvTDgZBjMSxtJu6QYxLMcrgwgrzpJxZCQZDZD&limit=20&since=2015-07-01&until=2015-11-25
class FBPostsWithSummary(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_posts_with_summary: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_posts_with_summary(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_posts_with_summary'
        return result

class FBPromotionalInfo(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_promotional_info: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_promotional_info(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_promotional_info'
        return result

#http://localhost:8080/twitteraccinfo?ids=[%271123728482,5539932%27]&tokens={%27consumer_key%27:%27xHl7DEIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27eVxjTk7d4Z41VQ2Kt7kcOF6aFjTQqqiWIKgM8xhqkMYoE8Pxmq%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSLytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVwP30at1fij9dFm4hL02tpCUFxbj%27}
class TwitterAccInfo(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received twitter_acc_info: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.twitter_acc_info(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed twitter_acc_info'
        return result

#http://localhost:8080/hashtag?hashtag=%27%23get%27&tokens={%27consumer_key%27:%27xHl7DIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27xHl7DEIJjH8NM2kn8Q9EddGy%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSL9ytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVP30at1fij9dFm4hL02tpCUFxbj%27}
class BuildWordCloud(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud'
        return result


#http://localhost:8080/buildwordcloudFB?token=%27CAACEdEose0cBAG6UIEov65x3tzohGZCON6UIAWInumkOZAInzw0ovs21Oh8090YV6hWP3pUTT853Q7wdSK8UfOCTqN68veN1bCnhWTn5hoZBnZBdI7vo4QMq5mtS5qZBJmfxnaZASiRfS9j2dlBmFqeWHd8faJrNij1QQasn22ZAcXMvB57KdbkWIUhTGxuvyQ1TTZBEewC9iQZDZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%2710153455424900369%27]
class BuildWordCloudFB(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud_fb: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud_fb(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud_fb'
        return result

#http://localhost:8080/buildwordcloudrt?tokens=%27rr%27&hashtag=earthquake&unique_id=eq
class BuildWordCloudRT(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud_rt: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud_rt(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud_rt'
        return result

#http://localhost:8080/sentimentanalysis?tokens=%27CAACEdEose0cBAGDfAva3R79cV1CmNBSObNfAkZBz5Xbe4fGXN353jzynphA0ZBJ251mFce0CTJyZCSlfjQoIuuWJNJKrH6uQtNCeAWhOOZCWfX4VuuZBUvpx0QexOKMQG8E82Weqpi6wNziEXMJlzwGnhka1vbxCJZBt7vHHx4BDuUEWjWO3DZCbz3MbqfINbkZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%27854964737921809_908260585925557%27,%27854964737921809_865555086862774%27]
class SentimentAnalysis(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received sentiment_analysis: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.sentiment_analysis(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed sentiment_analysis'
        return result

#http://localhost:8080/buildbipartite?token=%27CAACEdEose0cBAGDfAva3R79cV1CmNBSObNfAkZBz5Xbe4fGXN353jzynphA0ZBJ251mFce0CTJyZCSlfjQoIuuWJNJKrH6uQtNCeAWhOOZCWfX4VuuZBUvpx0QexOKMQG8E82Weqpi6wNziEXMJlzwGnhka1vbxCJZBt7vHHx4BDuUEWjWO3DZCbz3MbqfINbkZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%27854964737921809_908260585925557%27,%27854964737921809_865555086862774%27]
class BuildBiPartite(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_bi_partite: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_bi_partite(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_bi_partite'
        return result

#http://localhost:8080/linear?dbtype=MSSQL&db=Demo&table=OrdersDK&x=Unit_Price&y=Sales&predict=[5,8]
class LinearRegression():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received linear_regression: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DiginAlgo.DiginAlgo_service.linear_regression(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed linear_regression'
        return result

class Upload():
    def POST(self):
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received file_upload: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        result = scripts.FileUploadService.FileUploadService.file_upload(web.input())
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed file_upload'
        return result

#http://localhost:8080/generateboxplot?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery
class BoxPlotGeneration():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received box_plot_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received box_plot_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.box_plot_generation(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed box_plot_generation'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed box_plot_generation')
        return result

#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery&ID=11
#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&SecurityToken=7749e9d64eea8acf84bc3ee4368cec95&Domain=duosoftware.com
class HistogramGeneration():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received histogram_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.histogram_generation(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed histogram_generation'
        return result

#http://localhost:8080/generatebubble?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Allowance&s=salary&c=gender&ID=3
#http://localhost:8080/bubblechart?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Allowance&s=salary&c=gender
class BubbleChart():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received bubble_chart: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.bubble_chart(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed bubble_chart'
        return result

class ExecuteQuery():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received execute_query: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.execute_query(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed execute_query'
        return result

class GetFields():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_fields: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.get_fields(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_fields'
        return result

class GetTables():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.get_tables(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables')
        return result

class GetTables():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.get_tables(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables')
        return result

class GetLayout():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_layout: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_layout(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_layout'
        return result

class GetQueries():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_queries: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_queries: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_queries(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_queries'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_queries')
        return result

class GetReportNames():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_report_names: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_report_names: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_report_names(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names')
        return result

class ExecuteKTR():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        Domain = web.input().Domain
        authResult = Auth.GetSession(secToken,Domain)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.executeKTR(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names')
        return result

if __name__ == "__main__":
    try:
        p = Process(target=scripts.DigINCacheEngine.CacheGarbageCleaner.initiate_cleaner)
        p.start()
    except:
        print "Error initializing cache"

    app = web.application(urls, globals())
    app.run()
