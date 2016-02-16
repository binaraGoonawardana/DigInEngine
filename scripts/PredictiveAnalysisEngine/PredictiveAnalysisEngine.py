__author__ = 'Marlon Abeykoon'
__version__ = '1.0.1.2'

import web
import json
import datetime
import ForecastingProcessor as FP
import sys
sys.path.append("...")
import BigQueryHandler as BQ


urls = (
    '/forecasting(.*)', 'Forecasting',
    '/forecast(.*)', 'Forecasting_1'
)

app = web.application(urls, globals())


#http://localhost:8080/forecast?model=Additive&pred_error_level=0.0001&alpha=0&beta=53&gamma=34&fcast_days=30&table_name=[Demo.forcast_superstoresales]&field_name_d=Date&field_name_f=Sales&steps_pday=1&m=7&interval=Daily
class Forecasting_1():
    def GET(self, r):
        fcast_days = int(web.input().fcast_days)
        timesteps_per_day = int(web.input().steps_pday)
        pred_error_level = float(web.input().pred_error_level)
        model = str(web.input().model)
        m = int(web.input().m)
        alpha = web.input().alpha
        beta = web.input().beta
        gamma = web.input().gamma
        table_name = web.input().table_name
        field_name_date = web.input().field_name_d
        field_name_forecast = web.input().field_name_f
        interval = str(web.input().interval)
        null = None

        if interval == 'Daily':
            query = "SELECT TIMESTAMP_TO_SEC({0}) as date, SUM({1}) as value from {2} group by date order by date".format(field_name_date,field_name_forecast,table_name)
        # elif plot_interval == 'Monthly':
        #     query = "SELECT TIMESTAMP_TO_SEC(TIMESTAMP(concat(string(STRFTIME_UTC_USEC({0}, '%Y')),'-', string(STRFTIME_UTC_USEC({1}, '%m')),'-','01'))) as date, FLOAT(SUM({2})) as value FROM {3} GROUP BY  date ORDER BY  date".format(field_name_date, field_name_date, field_name_forecast, table_name)
            result = json.loads(BQ.execute_query(query))

            datapoints = []
            for row in result:
                datapoints.append([row['value'], row['date']])
            data_in = [{"target": "Historical_values", "datapoints": datapoints}]

            #translate the data.  There may be better ways if you're
            #prepared to use pandas / input data is proper json
            time_series = data_in[0]["datapoints"]

            epoch_in = []
            Y_observed = []

            for (y,x) in time_series:
                if y and x:
                    epoch_in.append(x)
                    Y_observed.append(y)

            #Pass in the number of days to forecast
            #fcast_days = 30
            res = FP.holt_predict(Y_observed,epoch_in,model,m,fcast_days,pred_error_level,timesteps_per_day)
            data_out = data_in + res

            for i in range( len( data_out ) ):
                if data_out[i]['target'] != 'RMSE' and data_out[i]['target'] != 'TotalForecastedVal':
                    for j in range(len(data_out[i]['datapoints'])):
                        lst = list(data_out[i]['datapoints'][j])
                        #casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d %H:%M:%S')
                        casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d')
                        data_out[i]['datapoints'][j] = (lst[0],casted_datetime)

            # print json.dumps(data_out)
            # import matplotlib.pyplot as plt
            # plt.plot(epoch_in,Y_observed)
            # m,tstamps = zip(*res[0]['datapoints'])
            # u,tstamps = zip(*res[1]['datapoints'])
            # l,tstamps = zip(*res[2]['datapoints'])
            # plt.plot(tstamps,u, label='upper')
            # plt.plot(tstamps,l, label='lower')
            # plt.plot(tstamps,m, label='mean')
            # plt.show()
            return json.dumps(data_out)

        elif interval == 'Monthly':

            #query = "SELECT TIMESTAMP_TO_SEC({0}) as date, SUM({1}) as value from {2} group by date order by date".format(field_name_date,field_name_forecast,table_name)
            query = "SELECT TIMESTAMP_TO_SEC(TIMESTAMP(concat(string(STRFTIME_UTC_USEC({0}, '%Y')),'-', string(STRFTIME_UTC_USEC({1}, '%m')),'-','01'))) as date, FLOAT(SUM({2})) as value FROM {3} GROUP BY  date ORDER BY  date".format(field_name_date, field_name_date, field_name_forecast, table_name)

            #print query
            result = json.loads(BQ.execute_query(query))


            datapoints = []
            for row in result:
                datapoints.append([row['value'], row['date']])
            data_in = [{"target": "average", "datapoints": datapoints}]

            #translate the data.  There may be better ways if you're
            #prepared to use pandas / input data is proper json
            time_series = data_in[0]["datapoints"]

            epoch_in = []
            Y_observed = []

            for (y,x) in time_series:
                if y and x:
                    epoch_in.append(x)
                    Y_observed.append(y)

            #Pass in the number of days to forecast
            #fcast_days = 30
            res = FP.holt_predict(Y_observed,epoch_in,model,m,fcast_days,pred_error_level,timesteps_per_day,)
            data_out = data_in + res
            for i in range( len( data_out ) ):
                if data_out[i]['target'] != 'RMSE' and data_out[i]['target'] != 'TotalForecastedVal':
                    for j in range(len(data_out[i]['datapoints'])):
                        lst = list(data_out[i]['datapoints'][j])
                        #casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d %H:%M:%S')
                        casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d')
                        data_out[i]['datapoints'][j] = (lst[0],casted_datetime)
            # print json.dumps(data_out)
            # import matplotlib.pyplot as plt
            # plt.plot(epoch_in,Y_observed)
            # m,tstamps = zip(*res[0]['datapoints'])
            # u,tstamps = zip(*res[1]['datapoints'])
            # l,tstamps = zip(*res[2]['datapoints'])
            # plt.plot(tstamps,u, label='upper')
            # plt.plot(tstamps,l, label='lower')
            # plt.plot(tstamps,m, label='mean')
            # plt.show()
            return json.dumps(data_out)




#localhost:8080/forecasting?interval=MONTHLY&table_name=Demo.forcast_superstoresales&field_name_d=Date&field_name_u=OrderQuantity&field_name_f=Sales&steps=4
# class Forecasting():
#     def GET(self, r):
#         interval = web.input().interval
#         steps = int(web.input().steps)
#         table_name = web.input().table_name
#         field_name_date = web.input().field_name_d
#         field_name_forecast = web.input().field_name_f
#
#         if interval == 'MONTHLY':
#             query = "SELECT integer(STRFTIME_UTC_USEC({0}, '%Y')) as year, integer(STRFTIME_UTC_USEC({1}, '%m')) as month, FLOAT(SUM({2})) as sales " \
#                     "FROM [{3}] GROUP BY year, month ORDER BY year, month"\
#                 .format(field_name_date,field_name_date,field_name_forecast, table_name)
#             data_set = json.loads(BQ.execute_query(query))
#             output = []
#
#             for row in data_set:
#                 output.append(row.values())
#
#             result = HW.holt_winter(np.matrix(output))
#             print result
#
#             final_result = data_set
#             max_year = int(data_set[-1]['year'])
#             max_month = int(data_set[-1]['month'])
#             #for i in range(data_set[len(data_set),steps]):
#             for i in range(0,steps):
#                 max_year = max_year+1 if max_month+1 > 12 else max_year
#                 max_month = 1 if max_month+1 > 12 else max_month+1
#                 final_result.append({u'year': max_year , u'sales': '', u'month': max_month, })
#             #print len(data_set)
#             #print len(final_result)
#
#             for i in range(0,len(result)):
#                 final_result[i]['forecasted_value'] = result[i,0]
#                 final_result[i]['forecasted_lower'] = result[i,1]
#                 final_result[i]['forecasted_upper'] = result[i,2]
#
#             return json.dumps(final_result)


if __name__ == "__main__":
    app.run()

