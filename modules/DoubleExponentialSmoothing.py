__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

def double_exponential_smoothing_additive(series, alpha, beta, n_predict):

    result = [series[0]]
    k = 1
    for n in range(1, len(series) + n_predict):

        if n == 1:
            l, t = series[0], series[1] - series[0]
        # we are forecasting
        if n >= len(series):
            y = result[-1]
            k += 1
        else:
            y = series[n]

        last_l = l
        l = alpha*y + (1-alpha)*(l+t)
        t = beta*(l-last_l) + (1-beta)*t
        result.append(l+(k*t))

    return result

def double_exponential_smoothing_multiplicative(series, alpha, beta, n_predict):

    result = [series[0]]
    k = 1
    for n in range(1, len(series) + n_predict):

        if n == 1:
            l, t = series[0], series[1]/series[0]
        # we are forecasting
        if n >= len(series):
            y = result[-1]
            k += 1
        else:
            y = series[n]

        last_l = l
        l = alpha*y + (1-alpha)*(l*t)
        t = beta*(l/last_l) + (1-beta)*t
        result.append(l*(t**k))

    return result

