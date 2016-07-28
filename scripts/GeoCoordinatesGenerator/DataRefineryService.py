__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
# import modules.SQLQueryHandler as sql
import modules.test_sqlsplit as splitter
import web

urls = (
    '/refinegeoloc(.*)', 'RefineGeoLocations'
)

app = web.application(urls, globals())

class RefineGeoLocations(web.storage):
    def GET(self, r):

        table_name = web.input().table_name
        field_name1 = web.input().f_name1
        field_name2 = web.input().f_name2
        field_name3 = web.input().f_name3
        field_name4 = web.input().f_name4
        deliminator = web.input().delim

        data = splitter.sqlsplit(table_name, field_name1, field_name2, field_name3, field_name4, deliminator)

        return data

