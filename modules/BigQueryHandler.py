__author__ = 'Sajeetharan'
__version__ = '1.0.1.1'
from bigquery import get_client
import sys
sys.path.append("...")
import configs.ConfigHandler as conf
import scripts.DigInRatingEngine.DigInRatingEngine as dre
import threading
from googleapiclient import discovery
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
project_id = datasource_settings['PROJECT_ID']
service_account = datasource_settings['SERVICE_ACCOUNT']
key = datasource_settings['KEY']

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
    query = ""
    project_id = datasource_settings['PROJECT_ID']
    service_account = datasource_settings['SERVICE_ACCOUNT']
    key = datasource_settings['KEY']
except Exception, err:
    print err

def execute_query(querystate, offset=None, limit=None, user_id=None, tenant=None):
          query = querystate
          try:
              client = get_client(project_id, service_account=service_account,
                                private_key_file=key, readonly=False)
              job_id, totalBytesProcessed, statistics, download_bytes, _ = client.query(query, timeout=60)
              totalBytesBilled = statistics['query']['totalBytesBilled']
              #outputBytes = statistics['load']['outputBytes']
              usages = {'totalBytesProcessed':totalBytesProcessed,
                        'totalBytesBilled':totalBytesBilled,
                        'download_bq' : download_bytes
                        }
              obj = dre.RatingEngine(user_id, tenant,job_id,**usages)
              p1 = threading.Thread(target=obj.set_usage(), args=())
              p1.start()
          except Exception, err:
              print err
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

def get_tables(dataset_ID):
          datasetID = dataset_ID

          try:
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
              result  = client.get_all_tables(datasetID)
          except Exception, err:
              print err
              raise
          return result

def get_table(dataset_ID, table):
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
              result  = client.get_table(dataset_ID,table)
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


def Insert_Data(datasetname,table_name,DataObject,user_id=None,tenant=None):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False, swallow_results=False)

          insertObject = DataObject
          try:
              upload_size, result  = client.push_rows(datasetname,table_name,insertObject)

          except Exception, err:
              print err
              raise
          storage_query = "SELECT * FROM [{0}.__TABLES__] WHERE table_id = '{1}' limit 1".format(datasetname,table_name)
          storage_bq = execute_query(storage_query,user_id=0,tenant='DigInEngine')[0]['size_bytes']
          usages = {'upload_size_bq': upload_size,
                    'storage_bq': storage_bq}
          try:
              obj = dre.RatingEngine(user_id, tenant, **usages)
              p1 = threading.Thread(target=obj.set_usage(), args=())
              p1.start()

          except Exception, err:
            print err

          return result

def get_datasets():
    client = get_client(project_id, service_account=service_account,
                        private_key_file=key, readonly=False, swallow_results=False)
    datasets = client.get_datasets()
    return datasets

# Method added by Thivatharan

def inser_data(schema,dataset_name,table_name,file_path,filename,user_id=None,tenant=None):

    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account, key)
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

    insert_request = bigquery.jobs().insert(
        projectId=project_id,
        # https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource
        body={
            'configuration': {
                'load': {
                    'schema': {
                        'fields': schema
                    },
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': dataset_name,
                        'tableId': table_name
                    },
                    'sourceFormat': 'CSV',
                }
            }
        },
        media_body=MediaFileUpload(
            file_path + '/' + filename,
            mimetype='application/octet-stream'))

    job = insert_request.execute()
    print('Waiting for job to finish...')

    status_request = bigquery.jobs().get(
        projectId= job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = status_request.execute(num_retries=3)
        if result['status']['state'] == 'DONE':
            if result['status'].get('errors'):
                raise RuntimeError('\n'.join(
                    e['message']for e in result['status']['errors']))
            print('Job complete.')
            usages = {'upload_bq': int(result['statistics']['load']['inputFileBytes']),
                      'storage_bq': int(result['statistics']['load']['inputFileBytes'])}
            try:
                obj = dre.RatingEngine(user_id, tenant, **usages)
                p1 = threading.Thread(target=obj.set_usage(), args=())
                p1.start()

            except Exception, err:
                print err
            return result

def delete_table(dataset, table):
    client = get_client(project_id, service_account=service_account,
                        private_key_file=key, readonly=False, swallow_results=False)
    result = client.delete_table(dataset, table)
    return result

def check_table(dataset, table):
    client = get_client(project_id, service_account=service_account,
                        private_key_file=key, readonly=False, swallow_results=False)
    result = client.check_table(dataset, table)
    return result