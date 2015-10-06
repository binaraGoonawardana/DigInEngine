__author__ = 'Sajeetharan'
import web
from bigquery import get_client
urls = (
    '/engine/(.*)', 'execute_query'

)
app = web.application(urls, globals())
query = ""
class execute_query:
    def GET(self,querystate):
          query = querystate
# BigQuery project id as listed in the Google Developers Console.
          project_id = 'digin-1085'
# Service account email address as listed in the Google Developers Console.
          service_account = '1011363222700-epqo6lmkl67j6u1qafha9dlke0pmcck3@developer.gserviceaccount.com'
# PKCS12 or PEM key provided by Google.
          key = 'Digin-d245213e7da9.p12'
          client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
        # Submit an async query.
          job_id, _results = client.query(query)
        # Check if the query has finished running.
          complete, row_count = client.check_job(job_id)
        # Retrieve the results.
          results = client.get_query_rows(job_id)
          return results
if  __name__ == "__main__":
    app.run()