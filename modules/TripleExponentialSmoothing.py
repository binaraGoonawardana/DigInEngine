__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

def initial_trend(series, len_season):

    sum = 0.0
    for i in range(len_season):
        sum += float(series[i+len_season] - series[i]) / len_season
    return sum / len_season

def initial_seasonal_components(series, len_season):

    seasons = {}
    season_avg = []
    n_seasons = int(len(series)/len_season)
    # compute season averages
    for j in range(n_seasons):
        season_avg.append(sum(series[len_season*j:len_season*j+len_season])/float(len_season))
    # compute initial values
    for i in range(len_season):
        sum_of_vals_over_avg = 0.0
        for j in range(n_seasons):
            sum_of_vals_over_avg += series[len_season*j+i]-season_avg[j]
        seasons[i] = sum_of_vals_over_avg/n_seasons
    return seasons

def triple_exponential_smoothing(series, len_season, alpha, beta, gamma, n_predict):

    result = []
    seasons = initial_seasonal_components(series, len_season)
    for i in range(len(series)+n_predict):
        if i == 0: # initial values
            smooth = series[0]
            trend = initial_trend(series, len_season)
            result.append(series[0])
            continue
        if i >= len(series): # we are forecasting
            m = i - len(series) + 1
            result.append((smooth + m*trend) + seasons[i % len_season])
        else:
            val = series[i]
            last_smooth, smooth = smooth, alpha*(val-seasons[i % len_season]) + (1-alpha)*(smooth+trend)
            trend = beta * (smooth-last_smooth) + (1-beta)*trend
            seasons[i % len_season] = gamma*(val-smooth) + (1-gamma)*seasons[i % len_season]
            result.append(smooth+trend+seasons[i % len_season])

    return result
