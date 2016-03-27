__author__ = 'Sajeetharan'

from bigquery import get_client
import sys
sys.path.append("...")
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
query = ""
project_id = datasource_settings['PROJECT_ID']
service_account = datasource_settings['SERVICE_ACCOUNT']
key = datasource_settings['KEY']

def execute_query(querystate):
          query = querystate
          try:
              client = get_client(project_id, service_account=service_account,
                                private_key_file=key, readonly=False)
              job_id, _results = client.query(query)
          except Exception, err:
              print err
              raise err
          complete, row_count = client.check_job(job_id)
          results = client.get_query_rows(job_id)
          return  results


def get_fields(dataset_name,table_name):
          fields = []
          datasetname = dataset_name
          tablename = table_name
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          results = client.get_table_schema(datasetname,tablename)
          for x in results:
            fields.append(x['name'])
          return fields

def get_table(dataset_ID):
          tables = []
          datasetID = dataset_ID
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          result  = client._get_all_tables(datasetID,cache=False)
          for x in result:
            tables.append(x['name'])
          return tables

def create_Table(dataset_name,table_name,schema):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          datasetname = dataset_name
          tablename = table_name
          try:
              result  = client.create_table(datasetname,tablename,schema)
              return result
          except Exception, err:
              return False


def Insert_Data(datasetname,table_name,DataObject):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)

          insertObject = DataObject
          try:
              result  = client.push_rows(datasetname,table_name,insertObject)
          except Exception, err:
              return False
          finally:
              return True