""""
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a
"""
from dune_client.api.extensions import ExtendedAPI
from dune_client.api.query import QueryAPI


class DuneClient(QueryAPI, ExtendedAPI):
    """
    An interface for Dune API with a few convenience methods
    combining the use of endpoints (e.g. run_query)

    Inheritance Hierarchy sketched as follows:

        DuneClient
        |
        |--- QueryAPI(BaseRouter)
        |       - Contains CRUD Operations on Queries
        |
        |--- ExtendedAPI
                | - Contains compositions of execution methods like `run_query`
                |
                |--- ExecutionAPI(BaseRouter)
                        - Contains query execution and result related methods.
    """
