__author__ = 'Marlon'
import json
from bigquery import get_client

query = ""
project_id = 'duo-world'
service_account = '53802754484-kcgm9tslt5udcagotvvokpehqqnrb868@developer.gserviceaccount.com'
key = 'DUO WORLD-e5a45513dd2b.p12'

def execute_query(querystate):
          query = querystate
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
          job_id, _results = client.query(query)
          complete, row_count = client.check_job(job_id)
          results = client.get_query_rows(job_id)
          return  json.dumps(results)


def get_fields(dataset_name,table_name):
          fields = []
          datasetname = dataset_name
          tablename = table_name
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          results = client.get_table_schema(datasetname,tablename)
          for x in results:
            fields.append(x['name'])
          return json.dumps(fields)

def get_table(dataset_ID):
          tables = []
          datasetID = dataset_ID
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
          result  = client._get_all_tables(datasetID,cache=False)
          for x in result:
            tables.append(x['name'])
          return tables