
import json
import web
from bigquery import get_client

urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'
)
app = web.application(urls, globals())

query = ""
project_id = 'duo-world'
service_account = '53802754484-kcgm9tslt5udcagotvvokpehqqnrb868@developer.gserviceaccount.com'
key = 'DUO WORLD-e5a45513dd2b.p12'

class execute_query:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          query = web.input().query
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          job_id, _results = client.query(query)
          complete, row_count = client.check_job(job_id)
          results = client.get_query_rows(job_id)
          return  json.dumps(results)

class get_Fields:
   def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          fields = []
          datasetname = web.input().datasetName
          tablename = web.input().tableName
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          results = client.get_table_schema(datasetname,tablename)
          for x in results:
            fields.append(x['name'])
          return json.dumps(fields)

class get_Tables:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          tables = []
          datasetID = web.input().dataSetID
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          result  = client._get_all_tables(datasetID,cache=False)

          print result
          tablesWithDetails =    result["tables"]
          print tablesWithDetails
          for inditable in tablesWithDetails:
              tables.append(inditable["id"])
          return json.dumps(tables)

if  __name__ == "__main__":
        app.run()