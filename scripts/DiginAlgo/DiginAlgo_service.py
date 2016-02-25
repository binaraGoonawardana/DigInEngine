__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import web
import ast
import sys
sys.path.append("...")
import DiginAlgo as DA

urls = (
    '/linear(.*)', 'linear'
)

app = web.application(urls, globals())

class linear():
    def GET(self,r):
        dbtype = web.input().dbtype
        db = web.input().db
        table = web.input().table
        x = web.input().x
        y = web.input().y
        predict = ast.literal_eval(web.input().predict)
        result = DA.get_data(dbtype, db,table, x, y, predict)

        return result

if __name__ == "__main__":
    app.run()

