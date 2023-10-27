"""Example test template."""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_codeocean_api.codeocean import CodeOceanClient
from requests import Response

from aind_codeocean_utils.api_handler import APIHandler

TEST_DIRECTORY = Path(os.path.dirname(os.path.realpath(__file__)))
MOCK_RESPONSE_FILE = TEST_DIRECTORY / "resources" / "co_responses.json"


class TestAPIHandler(unittest.TestCase):
    """Tests methods in APIHandler class"""

    @classmethod
    def setUpClass(cls):
        """Load mock_db before running tests."""

        co_mock_token = "abc-123"
        co_mock_domain = "https://aind.codeocean.com"

        with open(MOCK_RESPONSE_FILE) as f:
            json_contents = json.load(f)
        mock_search_all_data_assets_success_response = Response()
        mock_search_all_data_assets_success_response.status_code = 200
        mock_search_all_data_assets_success_response._content = json.dumps(
            json_contents["search_all_data_assets"]
        ).encode("utf-8")
        mock_co_client = CodeOceanClient(
            domain=co_mock_domain, token=co_mock_token
        )
        cls.mock_search_all_data_assets_success_response = (
            mock_search_all_data_assets_success_response
        )
        cls.api_handler = APIHandler(co_client=mock_co_client)
        cls.api_handler_dry = APIHandler(co_client=mock_co_client, dryrun=True)

    @patch(
        "aind_codeocean_api.codeocean.CodeOceanClient.search_all_data_assets"
    )
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_data_asset")
    @patch("logging.debug")
    @patch("logging.info")
    def test_update_tags(
        self,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_update: MagicMock,
        mock_get: MagicMock,
    ):
        """Tests update tags changes tags correctly."""
        mock_get.return_value = (
            self.mock_search_all_data_assets_success_response
        )
        mock_update_response = Response()
        mock_update_response.status_code = 200
        mock_update_response._content = b'{"message": "success"}'
        mock_update.return_value = mock_update_response
        response = self.api_handler.co_client.search_all_data_assets()
        data_assets = response.json()["results"]
        self.api_handler.update_tags(
            tags_to_remove=["test"],
            tags_to_add=["new_tag"],
            tags_to_replace={"ECEPHYS": "ecephys"},
            data_assets=data_assets,
        )

        expected_calls = [
            {
                "data_asset_id": "0faf14aa-13b9-450d-b26a-632935a4b763",
                "new_name": "ecephys_655019_2023-04-03_18-10-10",
                "new_tags": {"raw", "ecephys", "655019", "new_tag"},
            },
            {
                "data_asset_id": "84586a1c-79cc-4240-b340-6049fe8469c2",
                "new_name": "ecephys_655019_2023-04-03_18-17-09",
                "new_tags": {"ecephys", "655019", "new_tag", "raw"},
            },
            {
                "data_asset_id": "1936ae3a-73a8-422c-a7b1-1768732c6289",
                "new_name": (
                    "ecephys_661398_2023-03-31_17-01-09"
                    "_nwb_2023-06-01_14-50-08"
                ),
                "new_tags": {"new_tag"},
            },
            {
                "data_asset_id": "2481baf2-e9e8-4416-9a0b-d2ffe5782071",
                "new_name": (
                    "ecephys_660166_2023-03-16_18-30-14"
                    "_curated_2023-03-24_17-54-16"
                ),
                "new_tags": {"new_tag"},
            },
            {
                "data_asset_id": "fcd8bc84-bd48-4af7-8826-da5ceb5cdd3a",
                "new_name": "ecephys_636766_2023-01-25_00-00-00",
                "new_tags": {"new_tag"},
            },
            {
                "data_asset_id": "fc915970-5489-4b6d-af94-620b067cd2cd",
                "new_name": (
                    "ecephys_636766_2023-01-23_00-00-00"
                    "_sorted-ks2.5_2023-06-01_14-48-42"
                ),
                "new_tags": {"new_tag"},
            },
            {
                "data_asset_id": "63f2d2de-4af8-4397-94ab-9484c8e8c847",
                "new_name": (
                    "ecephys_622155_2022-05-31_15-29-16" "_2023-06-01_14-45-05"
                ),
                "new_tags": {"new_tag"},
            },
        ]
        actual_calls = [c.kwargs for c in mock_update.mock_calls]
        for row in actual_calls:
            row["new_tags"] = set(row["new_tags"])
        self.assertEqual(expected_calls, actual_calls)
        expected_debug_calls = [
            call(f"Updating data asset: {data_asset}")
            for data_asset in data_assets
        ]
        mock_log_debug.assert_has_calls(expected_debug_calls)
        mock_log_info.assert_has_calls(
            [call({"message": "success"}) for _ in data_assets]
        )

    @patch(
        "aind_codeocean_api.codeocean.CodeOceanClient.search_all_data_assets"
    )
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_data_asset")
    @patch("logging.debug")
    @patch("logging.info")
    def test_update_tags_with_nones(
        self,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_update: MagicMock,
        mock_get: MagicMock,
    ):
        """Tests that NoneType inputs are handled correctly."""
        mock_get.return_value = (
            self.mock_search_all_data_assets_success_response
        )
        mock_update_response = Response()
        mock_update_response.status_code = 200
        mock_update_response._content = b'{"message": "success"}'
        mock_update.return_value = mock_update_response
        response = self.api_handler.co_client.search_all_data_assets()
        data_assets = response.json()["results"]
        self.api_handler.update_tags(
            tags_to_add=["new_tag"],
            tags_to_replace={"ECEPHYS": "ecephys"},
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            tags_to_remove=["test"],
            tags_to_replace={"ECEPHYS": "ecephys"},
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            tags_to_remove=["test"],
            tags_to_add=["new_tag"],
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            tags_to_add=["new_tag"],
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            tags_to_remove=["test"],
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            tags_to_replace={"ECEPHYS": "ecephys"},
            data_assets=data_assets,
        )
        self.api_handler.update_tags(
            data_assets=data_assets,
        )
        data_assets_with_no_tags = [
            {
                "created": 1685645105,
                "description": "",
                "files": 10,
                "id": "63f2d2de-4af8-4397-94ab-9484c8e8c847",
                "last_used": 0,
                "name": "test_data_with_empty_tags",
                "sourceBucket": {
                    "bucket": "",
                    "origin": "local",
                    "prefix": "",
                },
                "state": "ready",
                "tags": [],
                "type": "dataset",
            },
            {
                "created": 1685645105,
                "description": "",
                "files": 10,
                "id": "63f2d2de-4af8-4397-94ab-9484c8e8c847",
                "last_used": 0,
                "name": "test_data_with_missing_field",
                "sourceBucket": {
                    "bucket": "",
                    "origin": "local",
                    "prefix": "",
                },
                "state": "ready",
                "type": "dataset",
            },
        ]
        self.api_handler.update_tags(
            tags_to_replace={"ECEPHYS": "ecephys"},
            data_assets=data_assets_with_no_tags,
        )
        mock_log_info.assert_called()
        mock_log_debug.assert_called()

    @patch(
        "aind_codeocean_api.codeocean.CodeOceanClient.search_all_data_assets"
    )
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_data_asset")
    @patch("logging.debug")
    @patch("logging.info")
    def test_update_tags_dryrun(
        self,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_update: MagicMock,
        mock_get: MagicMock,
    ):
        """Tests update tags changes tags correctly."""
        mock_get.return_value = (
            self.mock_search_all_data_assets_success_response
        )
        response = self.api_handler_dry.co_client.search_all_data_assets()
        data_assets = response.json()["results"]
        self.api_handler_dry.update_tags(
            tags_to_remove=["test"],
            tags_to_add=["new_tag"],
            data_assets=data_assets,
        )

        mock_update.assert_not_called()
        expected_debug_calls = [
            call(f"Updating data asset: {data_asset}")
            for data_asset in data_assets
        ]
        mock_log_debug.assert_has_calls(expected_debug_calls)
        mock_log_info.assert_called()


if __name__ == "__main__":
    unittest.main()
