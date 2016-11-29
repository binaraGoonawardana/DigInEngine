__author__ = 'Sajeetharan'
__version__ = '1.0.1.3'

from bigquery import get_client
import sys
sys.path.append("...")
import configs.ConfigHandler as conf
import scripts.DigInRatingEngine.DigInRatingEngine as dre
import scripts.DigINCacheEngine.CacheController as db
import scripts.utils.DiginIDGenerator as idgen
import datetime
import json
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

def get_tables(security_level, user_id, tenant):

          query = "SELECT " \
                  "ds.id, " \
                  "ds.project_id, " \
                  "ds.datasource_id, " \
                  "ds.datasource_type, " \
                  "ds.schema, " \
                  "up.upload_id, " \
                  "up.upload_type, " \
                  "up.uploaded_datetime, " \
                  "up.modified_datetime, " \
                  "up.upload_user, " \
                  "up.file_name, " \
                  "acc.security_level, " \
                  "ds.created_datetime, " \
                  "ds.created_user, " \
                  "ds.created_tenant, " \
                  "acc.shared_by, " \
                  "acc.user_group_id " \
                  "FROM " \
                  "digin_component_access_details acc " \
                  "INNER JOIN " \
                  "digin_datasource_details ds " \
                  "ON acc.component_id = ds.id " \
                  "LEFT OUTER JOIN " \
                  "digin_datasource_upload_details up " \
                  "ON acc.component_id = up.datasource_id " \
                  "WHERE " \
                  "acc.type = 'datasource' " \
                  "AND ds.is_active = true " \
                  "AND acc.is_active = true " \
                  "AND project_id = '{0}' " \
                  "AND acc.user_id = '{1}' " \
                  "AND acc.domain = '{2}'".format(project_id, user_id, tenant)

          result = db.get_data(query)['rows']
          shared_users_query = "SELECT component_id, user_id, security_level " \
                               "FROM digin_component_access_details " \
                               "WHERE type = 'datasource' " \
                               "AND domain = '{0}' " \
                               "AND is_active = True " \
                               "AND user_group_id is null".format(tenant)

          shared_users = db.get_data(shared_users_query)['rows']

          shared_user_groups_query = "SELECT DISTINCT user_group_id, component_id, security_level " \
                                     "FROM digin_component_access_details " \
                                     "WHERE type = 'datasource' " \
                                     "AND domain = '{0}' " \
                                     "AND is_active = True " \
                                     "AND user_group_id is not null".format(tenant)

          shared_user_groups = db.get_data(shared_user_groups_query)['rows']

          datasources = []

          for datasource in result:
              shared_users_cleansed = []
              shared_user_groups_cleansed = []
              for index, item in enumerate(datasources):
                  if item['datasource_id'] == datasource[0]:
                      datasources[index]['file_uploads'].append({'upload_id': datasource[5],
                                                                 'uploaded_datetime': datasource[7],
                                                                 'modified_datetime': datasource[8],
                                                                 'uploaded_user': datasource[9],
                                                                 'file_name': datasource[10]})
                      break
              else:
                  if datasource[13] == user_id or security_level == 'admin':
                      shared_users_cleansed = next((item for item in shared_users if item[0] == datasource[0]), None)
                      shared_user_groups_cleansed = next((item for item in shared_user_groups if item[1] == datasource[0]), None)

                  d = {'datasource_id': datasource[0],
                       'datasource_name': datasource[2],
                       'datasource_type': datasource[3],
                       'schema': json.loads(datasource[4]),
                       'upload_type': datasource[6],
                       'file_uploads': [{'upload_id': datasource[5],
                                         'uploaded_datetime': datasource[7],
                                         'modified_datetime': datasource[8],
                                         'uploaded_user': datasource[9],
                                         'file_name': datasource[10]}],
                       'security_level': datasource[11],
                       'created_datetime': datasource[12],
                       'created_user': datasource[13],
                       'created_tenant': datasource[14],
                       'shared_by': datasource[15],
                       'shared_users': shared_users_cleansed,
                       'shared_user_groups': shared_user_groups_cleansed
                       }
                  datasources.append(d)

          return datasources

def get_table(dataset_ID, table):
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
              result  = client.get_table(dataset_ID,table)
              return result

def create_Table(dataset_name,table_name,schema, security_level, user_id=None, tenant=None, upload_id=None):
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          datasetname = dataset_name
          tablename = table_name
          try:
              client.create_table(datasetname,tablename,schema)
              print 'Table created successfully'
          except Exception, err:
              print err
              return False

          table_id = idgen.unix_time_millis_id(datetime.datetime.now())
          table_data = {'id': table_id,
                        'project_id': project_id,
                        'dataset_id': datasetname,
                        'datasource_id': tablename,
                        'schema': json.dumps(schema),
                        'datasource_type': 'table',
                        'created_user': user_id,
                        'created_tenant': tenant,
                        'is_active': True}

          table_access_data = {'component_id': table_id,
                               'user_id': user_id,
                               'type': 'datasource',
                               'domain': tenant,
                               'security_level': security_level,
                               'is_active': True
                                }
          try:
                db.insert_data([table_data], 'digin_datasource_details')
                db.insert_data([table_access_data], 'digin_component_access_details')
                print 'Table mapped to the user'
          except Exception, err:
              print err
              return False
          return table_id


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