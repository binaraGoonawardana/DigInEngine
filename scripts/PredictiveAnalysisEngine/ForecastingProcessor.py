__author__ = 'Manura Omal Bhagya'

import HoltWintersAlgorithm as HWA
import numpy as np
import scipy.stats as ss
#timesteps_per_day = 288
#http://stackoverflow.com/questions/31147594/how-do-you-create-a-linear-regression-forecast-on-time-series-data-in-python/31257836#31257836


def holt_predict(data, timestamps, type, m, forecast_days, pred_error_level = 0.0001, timesteps_per_day = 24):
    #m =7
    if type == 'Linear':

        forecast_timesteps = forecast_days*timesteps_per_day
        middle_predictions, alpha, beta, rmse = HWA.linear(data,int(forecast_timesteps))
        tot_f = sum(middle_predictions)
        cum_error = [beta+alpha]
        for k in range(1,forecast_timesteps):
            cum_error.append(cum_error[k-1] + k*beta + alpha)

        cum_error = np.array(cum_error)
        #Use some numpy multiplication to get the intervals
        var = cum_error * rmse**2
        # find the correct ppf on the normal distribution (two-sided)
        p = abs(ss.norm.ppf((1-pred_error_level)/2))
        interval = np.sqrt(var) * p
        upper = middle_predictions + interval
        lower = middle_predictions - interval
        fcast_timestamps = [timestamps[-1] + i * 86400 / timesteps_per_day for i in range(forecast_timesteps)]

        ret_value = []

        ret_value.append({'target':'Forecast','datapoints': zip(middle_predictions, fcast_timestamps)})
        ret_value.append({'target':'Upper','datapoints':zip(upper,fcast_timestamps)})
        ret_value.append({'target':'Lower','datapoints':zip(lower,fcast_timestamps)})
        ret_value.append({'target':'RMSE','value':rmse})
        ret_value.append({'target':'TotalForecastedVal','tot':tot_f})
        return ret_value

    elif type == 'Additive':

        forecast_timesteps = forecast_days*timesteps_per_day
        middle_predictions, alpha, beta,gamma, rmse = HWA.additive(data, m, int(forecast_timesteps))
        print gamma
        tot_f = sum(middle_predictions)
        cum_error = [beta+alpha]
        for k in range(1,forecast_timesteps):
            cum_error.append(cum_error[k-1] + k*beta + alpha)

        cum_error = np.array(cum_error)
        #Use some numpy multiplication to get the intervals
        var = cum_error * rmse**2
        # find the correct ppf on the normal distribution (two-sided)
        p = abs(ss.norm.ppf((1-pred_error_level)/2))
        interval = np.sqrt(var) * p
        upper = middle_predictions + interval
        lower = middle_predictions - interval

        #if interval == 'Daily':
        fcast_timestamps = [timestamps[-1] + i * 86400 / timesteps_per_day for i in range(forecast_timesteps)]
        #elif interval == 'Monthly':
            #fcast_timestamps = [timestamps[-1] + i * 86400 / timesteps_per_day for i in range(forecast_timesteps)]

        ret_value = []

        ret_value.append({'target':'Forecast','datapoints': zip(middle_predictions, fcast_timestamps)})
        ret_value.append({'target':'Upper','datapoints':zip(upper,fcast_timestamps)})
        ret_value.append({'target':'Lower','datapoints':zip(lower,fcast_timestamps)})
        ret_value.append({'target':'RMSE','value':rmse})
        ret_value.append({'target':'TotalForecastedVal','tot':tot_f})
        return ret_value

    elif type == 'Multiplicative':

        forecast_timesteps = forecast_days*timesteps_per_day
        middle_predictions, alpha, beta,gamma, rmse = HWA.multiplicative(data, m, int(forecast_timesteps))
        tot_f = sum(middle_predictions)
        cum_error = [beta+alpha]
        for k in range(1,forecast_timesteps):
            cum_error.append(cum_error[k-1] + k*beta + alpha)

        cum_error = np.array(cum_error)
        #Use some numpy multiplication to get the intervals
        var = cum_error * rmse**2
        # find the correct ppf on the normal distribution (two-sided)
        p = abs(ss.norm.ppf((1-pred_error_level)/2))
        interval = np.sqrt(var) * p
        upper = middle_predictions + interval
        lower = middle_predictions - interval
        fcast_timestamps = [timestamps[-1] + i * 86400 / timesteps_per_day for i in range(forecast_timesteps)]

        ret_value = []

        ret_value.append({'target':'Forecast','datapoints': zip(middle_predictions, fcast_timestamps)})
        ret_value.append({'target':'Upper','datapoints':zip(upper,fcast_timestamps)})
        ret_value.append({'target':'Lower','datapoints':zip(lower,fcast_timestamps)})
        ret_value.append({'target':'RMSE','value':rmse})
        ret_value.append({'target':'TotalForecastedVal','tot':tot_f})
        return ret_value

#print holt_predict(data,[464646],'additive',5)
