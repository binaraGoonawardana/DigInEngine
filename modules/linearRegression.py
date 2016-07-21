__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

from sklearn import linear_model

""" Simple Linear Regression - One independent & one dependent"""
def simple_liner_regression(x, y, predict = None):

    slgr = linear_model.LinearRegression()
    try:
        slgr.fit(x, y)
        fitted = slgr.predict(x).tolist()
        coef = slgr.coef_.tolist()
        intrcpt = slgr.intercept_
        """ Calculating R^2 value"""
        rsquared = slgr.score(x,y)
        if predict:
            try:
                predict_val = slgr.predict(predict).tolist()
            except Exception, err:
                print err
                raise
        else:
            predict_val = []

        result = {'fitted_values': fitted, 'Coeffients': coef, 'intercept': intrcpt, 'predicted': predict_val, 'Rsquared': rsquared}
        return result

    except Exception, err:
        raise

#TODO : test the performance of the algorithm


