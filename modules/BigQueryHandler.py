__author__ = 'Sajeetharan'

from bigquery import get_client
import sys
sys.path.append("...")
import configs.ConfigHandler as conf

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
    query = ""
    project_id = datasource_settings['PROJECT_ID']
    service_account = datasource_settings['SERVICE_ACCOUNT']
    key = datasource_settings['KEY']
except Exception, err:
    print err

def execute_query(querystate, offset=None, limit=None):
          query = querystate
          try:
              client = get_client(project_id, service_account=service_account,
                                private_key_file=key, readonly=False)
              job_id, _ = client.query(query, timeout=60)
          except Exception, err:
              raise err
          #complete, row_count = client.check_job(job_id)
          client.check_job(job_id)
          results = client.get_query_rows(job_id, offset=offset, limit=limit)
          return  results


def get_fields(dataset_name,table_name):
          fields = []
          datasetname = dataset_name
          tablename = table_name
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          results = client.get_table_schema(datasetname,tablename)
          for x in results:
              fieldtype = {'Fieldname': x['name'],
                    'FieldType':x['type']}
              fields.append(fieldtype)
          return fields

def get_table(dataset_ID):
          datasetID = dataset_ID

          try:
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
              result  = client.get_all_tables(datasetID)
          except Exception, err:
              print err
              raise
          return result

def create_Table(dataset_name,table_name,schema):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          datasetname = dataset_name
          tablename = table_name
          try:
              result  = client.create_table(datasetname,tablename,schema)
              return result
          except Exception, err:
              print err
              return False

def create_dataset(dataset_name):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          try:
               result  = client.create_dataset(dataset_name,None,None,None)
               return  result
          except Exception, err:
               print err
               return err


def Insert_Data(datasetname,table_name,DataObject):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False, swallow_results=False)

          insertObject = DataObject
          try:
              result  = client.push_rows(datasetname,table_name,insertObject)
          except Exception, err:
              print err
              raise
          return result