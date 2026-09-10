import unittest

import pytest

from dune_client.client import DuneClient
from dune_client.models import ContractSubmissionListResponse


@pytest.mark.e2e
class TestContractsIntegration(unittest.TestCase):
    """
    E2E tests for ContractsAPI endpoints.
    These tests require a valid DUNE_API_KEY. Submitting is not exercised here
    because every call creates a real decoding submission for the key's owner.
    """

    def setUp(self) -> None:
        self.dune = DuneClient()

    def test_list_contract_submissions(self):
        result = self.dune.list_contract_submissions(limit=5)

        self.assertIsInstance(result, ContractSubmissionListResponse)
        self.assertIsInstance(result.submissions, list)
        self.assertIsInstance(result.total, int)
        self.assertLessEqual(len(result.submissions), 5)
        for submission in result.submissions:
            self.assertTrue(submission.id)
            self.assertTrue(submission.blockchain_name)
            self.assertTrue(submission.status)

    def test_list_contract_submissions_with_filter(self):
        result = self.dune.list_contract_submissions(limit=5, status="rejected")

        self.assertIsInstance(result, ContractSubmissionListResponse)
        for submission in result.submissions:
            self.assertEqual(submission.status, "rejected")
