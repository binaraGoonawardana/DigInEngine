
import json
import web
from bigquery import get_client
import sys
sys.path.append("...")
import configs.ConfigHandler as conf

urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields'
)
app = web.application(urls, globals())

datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
query = ""
project_id = datasource_settings['PROJECT_ID']
service_account = datasource_settings['SERVICE_ACCOUNT']
key = datasource_settings['KEY']


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