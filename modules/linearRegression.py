__author__ = 'Manura Omal Bhagya'

from sklearn import linear_model
from scipy import sparse

def liner_regression(x, y, predict = None):

    if predict:

        lgr = linear_model.LinearRegression()
        lgr.fit(x, y)
        fitted = lgr.predict(x)
        coef = lgr.coef_.tolist()
        intrcpt = lgr.intercept_
        rsquared = lgr.score(x,y)
        predict_val = lgr.predict(predict).tolist()
    else:
        lgr = linear_model.LinearRegression()
        lgr.fit(x, y)
        fitted = lgr.predict(x)
        coef = lgr.coef_.tolist()
        intrcpt = lgr.intercept_
        predict_val = ''
        rsquared = lgr.score(x, y)

    result = {'fitted_values': fitted, 'Coeffients': coef, 'intercept': intrcpt, 'predicted': predict_val, 'Rsquared': rsquared}

    return result

#TODO : test the performance of the algorithm


