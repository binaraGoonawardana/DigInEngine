__author__ = 'Manura Omal Bhagya'
__version__ = '2.0.0.0'

from scipy.optimize import minimize


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


def initial_seasonal_components_additive(series, len_season):

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


def _additive_opt(params, args):

    alpha, beta, gamma = params[0], params[1], params[2]
    len_season, series = args[1], args[0]
    t_level = initial_level(series, len_season)
    t_trend = initial_trend(series, len_season)
    t_season = initial_seasonal_components_additive(series, len_season)
    estimated = [t_level + t_trend + t_season[0 % len_season]]
    sse = 0.00

    for i in range(1, len(series)):

        tlast_level, tlast_trend, tlast_season = t_level, t_trend, t_season[i % len_season]
        t_level = alpha * (series[i] - tlast_season) + (1 - alpha) * (tlast_level + tlast_trend)
        t_trend = beta * (t_level - tlast_level) + (1 - beta) * tlast_trend
        t_season[i % len_season] = gamma * (series[i] - tlast_level - tlast_trend) + (1 - gamma) * tlast_season
        estimated.insert(i, t_level + t_trend + t_season[i % len_season])
        sse += (series[i] - estimated[i - 1]) ** 2

    return sse


def _multiplicative_opt(params, args):

    alpha, beta, gamma = params[0], params[1], params[2]
    len_season, series = args[1], args[0]
    t_level = initial_level(series, len_season)
    t_trend = initial_trend(series, len_season)
    t_season = initial_seasonal_components_multiplicative(series, len_season)
    estimated = [(t_level + t_trend) * t_season[0 % len_season]]
    sse = 0.00

    for i in range(1, len(series)):

        tlast_level, tlast_trend, tlast_season = t_level, t_trend, t_season[i % len_season]
        t_level = alpha * (series[i] / tlast_season) + (1 - alpha) * (tlast_level + tlast_trend)
        t_trend = beta * (t_level - tlast_level) + (1 - beta) * tlast_trend
        t_season[i % len_season] = gamma * (series[i] / (tlast_level + tlast_trend)) + (1 - gamma) * tlast_season
        estimated.insert(i, (t_level + t_trend) * t_season[i % len_season])
        sse += (series[i] - estimated[i - 1]) ** 2

    return sse


def triple_exponential_smoothing_additive(series, len_season, alpha, beta, gamma, n_predict):

    if alpha == '' or beta == '' or gamma == '':

        est_params = minimize(_additive_opt, x0=[0.001, 0.001, 0.001], bounds=[(0, 1), (0, 1), (0, 1)],
                              args=[series, len_season], method='L-BFGS-B')
        if alpha != '':
            alpha = alpha
        else:
            alpha = est_params.x[0]
        if beta != '':
            beta = beta
        else:
            beta = est_params.x[1]
        if gamma != '':
            gamma = gamma
        else:
            gamma = est_params.x[2]
    est = [float(alpha), float(beta), float(gamma)]

    alpha, beta, gamma = est
    level = initial_level(series, len_season)
    trend = initial_trend(series, len_season)
    season = initial_seasonal_components_additive(series, len_season)

    result = []
    for i in range(len(series)):
        if i < len_season:
            result.append('')
            continue

        last_level, last_trend, last_season = level, trend, season[i % len_season]
        level = alpha * (series[i] - last_season) + (1 - alpha) * (last_level + last_trend)
        trend = beta * (level - last_level) + (1 - beta) * last_trend
        season[i % len_season] = gamma * (series[i] - last_level - last_trend) + (1 - gamma) * last_season
        result.insert(i, level + trend + season[i % len_season])

    for j in range(n_predict):
        k = len(series)+j
        result.insert(k, level + j*trend + season[k % len_season])

    return [result, est]


def triple_exponential_smoothing_multiplicative(series, len_season, alpha, beta, gamma, n_predict):
#todo alpha beta gamma should be changed

    if alpha == '' and beta == '' and gamma == '':

        est_params = minimize(_multiplicative_opt, x0=[0.001, 0.001, 0.001], bounds=[(0, 1), (0, 1), (0, 1)],
                                   args=[series, len_season], method='L-BFGS-B')
        if alpha != '':
            alpha = alpha
        else:
            alpha = est_params.x[0]
        if beta != '':
            beta = beta
        else:
            beta = est_params.x[1]
        if gamma != '':
            gamma = gamma
        else:
            gamma = est_params.x[2]
    est = [float(alpha), float(beta), float(gamma)]

    alpha, beta, gamma = est
    level = initial_level(series, len_season)
    trend = initial_trend(series, len_season)
    season = initial_seasonal_components_multiplicative(series, len_season)

    result = []
    for i in range(len(series)):
        if i < len_season:
            result.append('')
            continue

        last_level, last_trend, last_season = level, trend, season[i % len_season]
        level = alpha * (series[i] / last_season) + (1 - alpha) * (last_level + last_trend)
        trend = beta * (level - last_level) + (1 - beta) * last_trend
        season[i % len_season] = gamma * (series[i] / (last_level + last_trend)) + (1 - gamma) * last_season
        result.insert(i, (level + trend) * season[i % len_season])

    for j in range(n_predict):
        k = len(series)+j
        result.insert(k, (level + j*trend) * season[k % len_season])

    return [result, est]

