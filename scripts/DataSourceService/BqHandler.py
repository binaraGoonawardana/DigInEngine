__author__ = 'Thivatharan Jeganathan '
__version__ = '1.0.0.0'

import configs.ConfigHandler as conf
import google.cloud.bigquery as gcb
import scripts.DigINCacheEngine.CacheController as CC
from oauth2client.service_account import ServiceAccountCredentials
import modules.CommonMessageGenerator as comm
import sys

datasource_settings = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
project_id = datasource_settings['PROJECT_ID']
service_account = datasource_settings['SERVICE_ACCOUNT']
key = datasource_settings['KEY']


def sync_query(dataset_id,table_id,first_row_no,last_row_no,upload_id):

    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account, key)
    query = "DELETE FROM {0}.{1} WHERE _index_id >= {2} AND _index_id < {3}".format(dataset_id,table_id,first_row_no,last_row_no)
    client = gcb.Client(project=project_id,credentials=credentials)
    query_results = client.run_sync_query(query)
    query_results.use_legacy_sql = False
    query_results.run()

    page_token = None

    while True:
        rows, total_rows, page_token = query_results.fetch_data(
            max_results=10,
            page_token=page_token)

        for row in rows:
            print(row)

        if not page_token:
            break
    try:
        CC.update_data('digin_datasource_upload_details',
                       "WHERE upload_id ={0}".format(upload_id), is_deleted=True)

    except Exception, err:
        return comm.format_response(False, err, "error while deleting!", exception=sys.exc_info())