import os, sys
import json
import web
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
from bigquery import get_client

urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields'
)
app = web.application(urls, globals())

query = ""
project_id = 'thematic-scope-112013'
service_account = 'diginowner@thematic-scope-112013.iam.gserviceaccount.com'
key = 'Digin-f537471c3b66.p12'

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