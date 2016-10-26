__author__ = 'Manura Omal Bhagya'
__version__ = '2.0.0.0'

from scipy.optimize import minimize


def double_additive_opt(params, args):

    alpha, beta = params[0], params[1]
    series = args[0]
    t_level = series[0]
    t_trend = (series[1] - series[0] + series[2] - series[1] + series[3] - series[2])/3

    estimated = [t_level + t_trend]
    sse = 0.00

    for i in range(1, len(series)):

        tlast_level, tlast_trend = t_level, t_trend
        t_level = alpha * series[i] + (1 - alpha)*(tlast_level - tlast_trend)
        t_trend = beta * (t_level - tlast_level) + (1 - beta)*tlast_trend
        estimated.insert(i, t_level + t_trend)
        sse += (series[i] - estimated[i - 1]) ** 2

    return sse


def double_exponential_smoothing_additive(series, alpha, beta, n_predict):

    if alpha == '' and beta == '':

        est_params = minimize(double_additive_opt, x0=[0.001, 0.001], bounds=[(0, 1), (0, 1)],
                              args=[series], method='L-BFGS-B')
        if alpha != '':
            alpha = alpha
        else:
            alpha = est_params.x[0]
        if beta != '':
            beta = beta
        else:
            beta = est_params.x[1]

    est = [float(alpha), float(beta) , '']

    alpha, beta, gamma = est
    level = series[0]
    trend = (series[1] - series[0] + series[2] - series[1] + series[3] - series[2])/3

    result = []
    for i in range(len(series)):

        last_level, last_trend = level, trend
        level = alpha * series[i] + (1 - alpha)*(last_level - last_trend)
        trend = beta * (level - last_level) + (1 - beta)*last_trend

        result.insert(i, level + trend)

    for j in range(n_predict):
        k = len(series)+j
        result.insert(k, level + j*trend)

    return [result, est]


def double_multiplicative_opt(params, args):

    alpha, beta = params[0], params[1]
    series = args[0]
    t_level = series[0]
    t_trend = series[1] / series[0]

    estimated = [t_level * t_trend]
    sse = 0.00

    for i in range(1, len(series)):

        tlast_level, tlast_trend = t_level, t_trend
        t_level = alpha * series[i] + (1 - alpha)*(tlast_level * tlast_trend)
        t_trend = beta * (t_level / tlast_level) + (1 - beta)*tlast_trend
        estimated.insert(i, t_level * t_trend)
        sse += (series[i] - estimated[i - 1]) ** 2

    return sse


def double_exponential_smoothing_multiplicative(series, alpha, beta, n_predict):

    if alpha == '' and beta == '':

        est_params = minimize(double_multiplicative_opt, x0=[0.001, 0.001, 0.001], bounds=[(0, 1), (0, 1), (0, 1)],
                                   args=[series], method='L-BFGS-B')
        if alpha != '':
            alpha = alpha
        else:
            alpha = est_params.x[0]
        if beta != '':
            beta = beta
        else:
            beta = est_params.x[1]

    est = [float(alpha), float(beta), '']

    alpha, beta, gamma = est
    level = series[0]
    trend = series[1] / series[0]

    result = []
    for i in range(len(series)):

        last_level, last_trend = level, trend
        level = alpha * series[i] + (1 - alpha)*(last_level * last_trend)
        trend = beta * (level / last_level) + (1 - beta)*last_trend

        result.insert(i, level * trend)

    for j in range(n_predict):
        k = len(series)+j
        result.insert(k, level * (trend**j))

    return [result, est]
