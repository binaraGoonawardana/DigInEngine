__author__ = 'Sajeetharan'

import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import scripts.DigINCacheEngine.CacheController as CC
import web
import logging
import operator
import ast
import json

urls = (
    '/storeDigin(.*)', 'Store'
)

class Store:
    def POST(self,r):






