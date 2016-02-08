__author__ = 'Sajeetharan'
import os, sys
import modules.BigQueryHandler as BQ
import web
import logging
import operator
import ast
import json
import os, sys
import json
import web
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
from bigquery import get_client

urls = (
    '/aggregatefields(.*)', 'AggregateFields',
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration',
    '/coordinates/(.*)', 'get_coords',
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/pageoverview(.*)', 'FBOverview',
    '/demographicsinfo(.*)', 'FBPageUserLocations',
    '/fbpostswithsummary(.*)', 'FBPostsWithSummary',
    '/promtionalinfo(.*)', 'FBPromotionalInfo',
    '/twitteraccinfo(.*)', 'TwitterAccInfo',
    '/hashtag(.*)', 'BuildWordCloud',
    '/buildwordcloudrt(.*)', 'BuildWordCloudRT',
    '/streamingtweets(.*)', 'StreamingTweets'
)

