__author__ = 'Sajeetharan'
import web
from bigquery import get_client
urls = (
    '/GetTables(.*)', 'get_Tables'
)
app = web.application(urls, globals())
class get_Tables:
    def GET(self,r):
          tables = []
          datasetID = web.input().dataSetID
# BigQuery project id as listed in the Google Developers Console.
          project_id = 'publicdata'
# Service account email address as listed in the Google Developers Console.
          service_account = '53802754484-165oeshhumvils04dp338l8b7q1sn524@developer.gserviceaccount.com'
# PKCS12 or PEM key provided by Google.
          key = 'DUO WORLD-cc1abf2276ed.p12'
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
        # Submit an async query.
         # job_id, _results = client.query("SELECT  * FROM" + datasetname+".INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME ="+tablename)
        # Submit an async query.
          result  = client._get_all_tables(datasetID,cache=False)

          print result
          tablesWithDetails =    result["tables"]
          print tablesWithDetails
          for inditable in tablesWithDetails:
              tables.append(inditable["id"])
          return tables

if  __name__ == "__main__":
    app.run()