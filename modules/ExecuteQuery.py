__author__ = 'Sajeetharan'
import web
import json
from bigquery import get_client
urls = (
    '/executeQuery(.*)', 'execute_query'

)
app = web.application(urls, globals())
query = ""
class execute_query:
    def GET(self,querystate):
          query = web.input().query
# BigQuery project id as listed in the Google Developers Console.
          project_id = 'duo-world'
# Service account email address as listed in the Google Developers Console.
          service_account = '53802754484-kcgm9tslt5udcagotvvokpehqqnrb868@developer.gserviceaccount.com'
# PKCS12 or PEM key provided by Google.
          key = 'DUO WORLD-e5a45513dd2b.p12'
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
        # Submit an async query.
          job_id, _results = client.query(query)
        # Check if the query has finished running.
          complete, row_count = client.check_job(job_id)
        # Retrieve the results.
          results = client.get_query_rows(job_id)

          return  json.dumps(results)
if  __name__ == "__main__":
    app.run()