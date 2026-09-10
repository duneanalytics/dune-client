"""
Contract decoding endpoints via /v1/contracts/*
"""

from __future__ import annotations

from dune_client.api.base import BaseRouter
from dune_client.models import (
    ContractSubmissionInput,
    ContractSubmissionListResponse,
    DuneError,
    SubmitContractsResponse,
)

MAX_SUBMISSIONS_PER_REQUEST = 100


class ContractsAPI(BaseRouter):
    """
    Implementation of Contract Decoding endpoints
    https://docs.dune.com/api-reference/contracts/introduction
    """

    def submit_contracts(
        self, submissions: list[ContractSubmissionInput]
    ) -> SubmitContractsResponse:
        """
        https://docs.dune.com/api-reference/contracts/endpoint/decode
        Submit up to 100 contracts for decoding in one request.

        Each item is validated and queued independently; the response carries one
        result per item, matched by index, so one bad ABI does not fail the batch.
        Submissions are attributed to the user who created the API key. Multi-chain
        batches and resubmissions require a paid plan.

        Args:
            submissions: Between 1 and 100 contracts to submit

        Returns:
            SubmitContractsResponse with one ContractSubmissionResult per input
        """
        if not 0 < len(submissions) <= MAX_SUBMISSIONS_PER_REQUEST:
            msg = f"submissions must contain between 1 and {MAX_SUBMISSIONS_PER_REQUEST} items"
            raise ValueError(msg)

        response_json = self._post(
            route="/contracts/decode",
            params={"submissions": [submission.to_request() for submission in submissions]},
        )
        try:
            return SubmitContractsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "SubmitContractsResponse", err) from err

    def list_contract_submissions(
        self,
        limit: int = 50,
        cursor: str | None = None,
        blockchain_name: str | None = None,
        address: str | None = None,
        project_name: str | None = None,
        contract_name: str | None = None,
        status: str | None = None,
    ) -> ContractSubmissionListResponse:
        """
        https://docs.dune.com/api-reference/contracts/endpoint/list
        List the contract decoding submissions made by the user who created the
        API key, newest first.

        Args:
            limit: Maximum number of submissions to return (max 250)
            cursor: `next_cursor` from a previous response, to fetch the next page
            blockchain_name: Optional filter by blockchain, e.g. "ethereum"
            address: Optional filter by contract address
            project_name: Optional filter by project (namespace) name
            contract_name: Optional filter by contract name
            status: Optional filter by status (pending, approved, rejected, processed,
                    in_progress, cancelled, needs_manual_review)

        Returns:
            ContractSubmissionListResponse with submissions, total count and next_cursor
        """
        params: dict[str, int | str] = {"limit": limit}
        optional_params = {
            "cursor": cursor,
            "blockchain_name": blockchain_name,
            "address": address,
            "project_name": project_name,
            "contract_name": contract_name,
            "status": status,
        }
        params.update({key: value for key, value in optional_params.items() if value is not None})

        response_json = self._get(route="/contracts/submissions", params=params)
        try:
            return ContractSubmissionListResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ContractSubmissionListResponse", err) from err
