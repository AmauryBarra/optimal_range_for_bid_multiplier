from google.cloud import bigquery
from .BQconfig import BIGQUERY_PROJECT, BIGQUERY_LOCATION

tables = {
    "bids":"remerge.bids_V3",
    "campaigns":"postgres.campaigns",
    "multipliers":"bid_multipliers"
}

class Client:
    def __init__(self, tables=tables, write=False):
        self.bq = bigquery.Client(
            project = BIGQUERY_PROJECT,
            location = BIGQUERY_LOCATION
        )
        self.tables = tables

    def query(self, query, params, destination=None):
        job_config = bigquery.QueryJobConfig(
            query_parameters = params,
            destination = destination
        )
        job=self.bg.query(query, job_config=job_config)
        result = job.result()

        return result

    def _query(self, query, params):
        return self.query(query, params).to_dataframe()


    def get_data(self, query, attribute, start_time, end_time):
        query = sql_query(query).format(
            campaigns=self.tables["campaigns"],
            multipliers=self.tables

        )

        start_date = start_time.date()
        end_date = end_time.date()

        params = [
            bigquery.ScalarQueryParameter("START_TIME", "DATETIME", start_time),
            bigquery.ScalarQueryParameter("END_TIME", "DATETIME", end_time),
            bigquery.ScalarQueryParameter("ATTRIBUTE", "STRING", attribute),
            ]

        return self._query(query, params)
    
    def sql_query(query):
        import importlib.resources as pkg_resources

        if query[-4:] == ".sql":
            query = pkg_resources.read_text(__package__, query)
        return query
