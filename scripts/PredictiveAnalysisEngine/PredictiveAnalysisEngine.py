__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import web
import json
import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ

urls = (
    '/forecasting(.*)', 'Forecasting',
    '/gethistoricalgroupeddata(.*)', 'GetHistoricalGroupedData'
)

app = web.application(urls, globals())

class Forecasting():
    def GET(self, r):
        interval = web.input().interval
        steps = web.input().steps
        table_name = web.input().table_name
        field_name_date = web.input().field_name_d
        field_name_forecast = web.input().field_name_f

        if interval == 'MONTHLY':
            query = "SELECT STRFTIME_UTC_USEC({0}, '%Y') as year, STRFTIME_UTC_USEC({1}, '%m') as month, SUM({2}) as sales " \
                    "FROM [{3}] GROUP BY year, month ORDER BY year, month"\
                .format(field_name_date,field_name_date,field_name_forecast, table_name)
            data_set = BQ.execute_query(query)
            print json.loads(data_set)
            return data_set


class GetHistoricalGroupedData():
    def GET(self, r):
        interval = web.input().interval
        table_name = web.input().table_name
        field_name_date = web.input().field_name_d
        field_name_unit = web.input().field_name_u
        field_name_amount = web.input().field_name_a

        if interval == 'MONTHLY':
            query = "SELECT STRFTIME_UTC_USEC({0}, '%Y') as year, STRFTIME_UTC_USEC({1}, '%m') as month, SUM({2}) as sales, SUM({3}) as tot_units " \
                    "FROM [{4}] GROUP BY year, month ORDER BY year, month"\
                    .format(field_name_date,field_name_date,field_name_amount,field_name_unit,table_name)
            data_set = BQ.execute_query(query)
            return data_set

if __name__ == "__main__":
    app.run()

