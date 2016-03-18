
import json
from bigquery import get_client
import sys
import sqlalchemy as sql
from sqlalchemy import text
import modules.CommonMessageGenerator as comm
sys.path.append("...")
import configs.ConfigHandler as conf


datasource_settings_bq = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
query = ""
project_id = datasource_settings_bq['PROJECT_ID']
service_account = datasource_settings_bq['SERVICE_ACCOUNT']
key = datasource_settings_bq['KEY']

datasource_settings_mssql = conf.get_conf('DatasourceConfig.ini','MS-SQL')
connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver=SQL+Server+Native+Client+11.0"\
                    .format(datasource_settings_mssql['UID'],datasource_settings_mssql['PWD'],datasource_settings_mssql['SERVER'],
                            datasource_settings_mssql['DATABASE'],datasource_settings_mssql['DRIVER'],datasource_settings_mssql['PORT'])
try:
    engine = sql.create_engine(connection_string)
    metadata = sql.MetaData()
    connection = engine.connect()
except:
    pass

def execute_query(params):

          query = params.query
          db = params.db
          columns = []

          if db == 'BigQuery':
               client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
               job_id, _results = client.query(query)
               complete, row_count = client.check_job(job_id)
               results = client.get_query_rows(job_id)
               return  comm.format_response(True,results,"",exception=None)

          elif db == 'MSSQL':
               data = []
               sql = text(query)
               result = connection.execute(sql)
               columns = result.keys()
               print columns
               results = []
               for row in result:
                  results.append(dict(zip(columns, row)))
               return  comm.format_response(True,results,"",exception=None)

          else:
               return "db not implemented"

def get_fields(params):

          fields = []
          datasetname = params.dataSetName
          tablename = params.tableName
          db = params.db

          if db == 'BigQuery':

                client = get_client(project_id, service_account=service_account,private_key_file=key, readonly=True)
                results = client.get_table_schema(datasetname,tablename)
                for x in results:
                  fields.append(x['name'])
                return  comm.format_response(True,fields,"",exception=None)
          elif db == 'MSSQL':
                fields = []
                datasetname = params.dataSetName
                tablename = params.tableName
                query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
                sql = text(query)
                result = connection.execute(sql)
                for row in result:
                  fieldtype = {'Fieldname': row[3],
                        'FieldType':row[7]}
                  fields.append(fieldtype)
                return  comm.format_response(True,fields,"",exception=None)
          else:
                return "db not implemented"


def get_tables(params):

          tables = []
          datasetID = params.dataSetName
          db = params.db

          if db == 'BigQuery':
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
              result  = client._get_all_tables(datasetID,cache=False)
              tablesWithDetails =    result["tables"]
              for inditable in tablesWithDetails:
                tables.append(inditable["id"])
              print(json.dumps(tables))
              tables = [i.split('.')[-1] for i in tables]
              return  comm.format_response(True,tables,"",exception=None)
          elif db == 'MSSQL':
              tables = []
              datasetID = params.dataSetName
              #connection_string = 'DRIVER={SQL Server};SERVER=192.168.1.83;DATABASE='+ datasetID +';UID=smsuser;PWD=sms';
              query = "SELECT * FROM information_schema.tables"
              result = connection.execute(query)
              for row in result:
                tables.append(row[2])
              return  comm.format_response(True,tables,"",exception=None)
          else:
              return "db not implemented"

