"""
Implementation of Pipeline API endpoints
"""

from __future__ import annotations

from dune_client.api.base import BaseRouter
from dune_client.models import DuneError, PipelineStatusResponse


class PipelineAPI(BaseRouter):
    """
    Implementation of Pipeline API Operations
    """

    def get_pipeline_status(self, pipeline_execution_id: str) -> PipelineStatusResponse:
        """GET pipeline execution status"""
        response_json = self._get(route=f"/pipelines/executions/{pipeline_execution_id}/status")
        try:
            return PipelineStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "PipelineStatusResponse", err) from err
