__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.1'


def initial_trend(series, len_season):

    sums = 0.0
    for i in range(len_season):
        sums += float(series[i+len_season] - series[i]) / len_season
    return sums / len_season


def initial_level(series, len_season):

    level = 0.0
    for i in range(len_season):
        level += float(series[i])
    return level/len_season


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


def initial_seasonal_components_multiplicative(series, len_season):

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
            sum_of_vals_over_avg += series[len_season*j+i]/season_avg[j]
        seasons[i] = sum_of_vals_over_avg/n_seasons
    return seasons


def triple_exponential_smoothing_additive(series, len_season, alpha, beta, gamma, n_predict):

    result = []
    seasons = initial_seasonal_components(series, len_season)
    for i in range(len(series)+n_predict):
        # initial values
        if i < len_season:
            level = initial_level(series, len_season)
            trend = initial_trend(series, len_season)
            result.append('')
            continue
        elif i == len_season:
            es = seasons[i-len_season]
            #result.append(level + trend + es)
        # forecasting
        if i >= len(series):
            m = i - len(series) + 1
            es = seasons[i % len_season]
            result.append((level + m*trend) + es)
        else:
            act = series[i]
            es = seasons[i % len_season]
            last_level, level = level, alpha*(act-es) + (1-alpha)*(level+trend)
            trend = beta * (level-last_level) + (1-beta)*trend
            seasons[i % len_season] = gamma*(act-level) + (1-gamma)*seasons[i % len_season]
            result.append(level+trend+seasons[i % len_season])

    return result


def triple_exponential_smoothing_multiplicative(series, len_season, alpha, beta, gamma, n_preds):

    result = []
    seasons = initial_seasonal_components_multiplicative(series, len_season)
    for i in range(len(series)+n_preds):
        # initial values
        if i < len_season:
            level = initial_level(series, len_season)
            trend = initial_trend(series, len_season)
            result.append('')
            continue
        elif i == len_season:
            es = seasons[i-len_season]
            #result.append((level + trend) * es)
        # forecasting
        if i >= len(series):
            m = len(series) - i + 1
            result.append((level + m*trend) * seasons[i % len_season])
        else:
            act = series[i]
            last_level, level = level, alpha*(act/seasons[i % len_season]) + (1-alpha)*(level+trend)
            trend = beta * (level-last_level) + (1-beta)*trend
            seasons[i % len_season] = gamma*(act/level) + (1-gamma)*seasons[i % len_season]
            result.append((level + trend) * seasons[i % len_season])

    return result

