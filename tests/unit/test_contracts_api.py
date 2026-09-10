import unittest
from unittest.mock import MagicMock

import pytest

from dune_client.api.contracts import ContractsAPI
from dune_client.models import (
    ContractSubmissionInput,
    ContractSubmissionListResponse,
    SubmitContractsResponse,
)

ABI = [{"type": "event", "name": "Transfer", "inputs": []}]


def submission(**overrides):
    fields = {
        "blockchain_name": "ethereum",
        "address": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        "project_name": "uniswap",
        "contract_name": "UniswapToken",
        "abi": ABI,
    }
    fields.update(overrides)
    return ContractSubmissionInput(**fields)


class TestContractsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.api = ContractsAPI(api_key="test_key")
        self.api._get = MagicMock()
        self.api._post = MagicMock()

    def test_submit_contracts_builds_body_and_parses_per_item_results(self):
        self.api._post.return_value = {
            "results": [
                {"index": 0, "submission_id": "sub_1", "status": "pending"},
                {"index": 1, "submission_id": "sub_0", "status": "pending", "replayed": True},
                {"index": 2, "error": "abi must be valid JSON"},
            ]
        }

        result = self.api.submit_contracts(
            [
                submission(is_proxy=True, idempotency_key="k/0"),
                submission(submission_type="upgrade", resubmission_reason="new ABI"),
                submission(abi="not json"),
            ]
        )

        self.api._post.assert_called_once()
        body = self.api._post.call_args.kwargs
        self.assertEqual(body["route"], "/contracts/decode")
        sent = body["params"]["submissions"]
        self.assertEqual(len(sent), 3)
        # Unset optional fields are omitted rather than sent as null.
        self.assertEqual(
            sent[0],
            {
                "blockchain_name": "ethereum",
                "address": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
                "project_name": "uniswap",
                "contract_name": "UniswapToken",
                "abi": ABI,
                "has_multiple_instances": False,
                "is_created_by_factory": False,
                "is_manual_abi": False,
                "is_proxy": True,
                "idempotency_key": "k/0",
            },
        )
        self.assertEqual(sent[1]["submission_type"], "upgrade")
        self.assertEqual(sent[1]["resubmission_reason"], "new ABI")
        self.assertNotIn("idempotency_key", sent[1])
        self.assertEqual(sent[2]["abi"], "not json")

        self.assertIsInstance(result, SubmitContractsResponse)
        self.assertEqual(result.results[0].submission_id, "sub_1")
        self.assertEqual(result.results[0].status, "pending")
        self.assertFalse(result.results[0].replayed)
        self.assertIsNone(result.results[0].error)
        self.assertTrue(result.results[1].replayed)
        self.assertEqual(result.results[2].index, 2)
        self.assertEqual(result.results[2].error, "abi must be valid JSON")
        self.assertIsNone(result.results[2].submission_id)

    def test_submit_contracts_rejects_empty_and_oversized_batches(self):
        with pytest.raises(ValueError, match="between 1 and 100"):
            self.api.submit_contracts([])
        with pytest.raises(ValueError, match="between 1 and 100"):
            self.api.submit_contracts([submission()] * 101)
        self.api._post.assert_not_called()

    def test_list_contract_submissions_minimal(self):
        self.api._get.return_value = {
            "submissions": [
                {
                    "id": "sub_1",
                    "blockchain_name": "ethereum",
                    "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                    "project_name": "uniswap",
                    "contract_name": "UniswapToken",
                    "status": "pending",
                    "submission_type": "new",
                    "created_at": "2026-09-10T11:04:18.724658Z",
                    "updated_at": "2026-09-10T11:04:18.724658Z",
                }
            ],
            "total": 1,
        }

        result = self.api.list_contract_submissions()

        self.api._get.assert_called_once_with(route="/contracts/submissions", params={"limit": 50})
        self.assertIsInstance(result, ContractSubmissionListResponse)
        self.assertEqual(result.total, 1)
        self.assertIsNone(result.next_cursor)
        self.assertEqual(result.submissions[0].id, "sub_1")
        self.assertEqual(result.submissions[0].status, "pending")
        self.assertIsNone(result.submissions[0].comment)
        self.assertIsNone(result.submissions[0].idempotency_key)

    def test_list_contract_submissions_with_filters_and_cursor(self):
        self.api._get.return_value = {
            "submissions": [
                {
                    "id": "sub_2",
                    "blockchain_name": "base",
                    "address": "0x1",
                    "project_name": "p",
                    "contract_name": "c",
                    "status": "rejected",
                    "submission_type": "upgrade",
                    "comment": "There is already a decoded contract at this address.",
                    "created_at": "2026-09-10T11:04:18Z",
                    "updated_at": "2026-09-10T11:05:18Z",
                    "idempotency_key": "k/1",
                }
            ],
            "total": 7,
            "next_cursor": "eyJpZCI6Ii4uLiJ9",
        }

        result = self.api.list_contract_submissions(
            limit=1,
            cursor="prev",
            blockchain_name="base",
            status="rejected",
        )

        self.api._get.assert_called_once_with(
            route="/contracts/submissions",
            params={"limit": 1, "cursor": "prev", "blockchain_name": "base", "status": "rejected"},
        )
        self.assertEqual(result.next_cursor, "eyJpZCI6Ii4uLiJ9")
        self.assertEqual(
            result.submissions[0].comment, "There is already a decoded contract at this address."
        )
        self.assertEqual(result.submissions[0].idempotency_key, "k/1")


if __name__ == "__main__":
    unittest.main()
