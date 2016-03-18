__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import ast
import sys
sys.path.append("...")
import DiginAlgo as DA


def linear_regression(params):
        dbtype = params.dbtype
        db = params.db
        table = params.table
        x = params.x
        y = params.y
        predict = ast.literal_eval(params.predict)

        result = DA.slr_get(dbtype, db,table, x, y, predict)
        return result
