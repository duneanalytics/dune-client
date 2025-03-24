""" "
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://docs.dune.com/api-reference/overview/introduction
"""

from dune_client.api.extensions import ExtendedAPI


class DuneClient(ExtendedAPI):
    """
    An interface for Dune API with a few convenience methods
    combining the use of endpoints (e.g. run_query)

    Inheritance Hierarchy sketched as follows:

        DuneClient
        |
        |--- ExtendedAPI
                |   - Contains compositions of execution methods like `run_query`
                |               (things like `run_query`, `run_query_csv`, etc..)
                |   - make use of both Execution and Query APIs
                |
                |--- ExecutionAPI(BaseRouter)
                |        - Contains query execution methods.
                |
                |--- QueryAPI(BaseRouter)
                |       - Contains CRUD Operations on Queries
    """
