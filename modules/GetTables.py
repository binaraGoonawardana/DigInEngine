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
          service_account = '1011363222700-epqo6lmkl67j6u1qafha9dlke0pmcck3@developer.gserviceaccount.com'
# PKCS12 or PEM key provided by Google.
          key = 'Digin-d245213e7da9.p12'
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
        # Submit an async query.
         # job_id, _results = client.query("SELECT  * FROM" + datasetname+".INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME ="+tablename)
        # Submit an async query.
          result  = client._get_all_tables(datasetID,cache=False)


          for x in result:
            tables.append(x['name'])


          return tables

if  __name__ == "__main__":
    app.run()