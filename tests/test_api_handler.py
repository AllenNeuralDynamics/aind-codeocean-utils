"""Example test template."""

import datetime
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from codeocean import CodeOcean
from codeocean.data_asset import DataAsset, DataAssetSearchParams

from aind_codeocean_utils.api_handler import APIHandler

TEST_DIRECTORY = Path(os.path.dirname(os.path.realpath(__file__)))
MOCK_RESPONSE_FILE = TEST_DIRECTORY / "resources" / "iterator_response.json"
MOCK_RESPONSE_FILE2 = TEST_DIRECTORY / "resources" / "co_responses.json"


class TestAPIHandler(unittest.TestCase):
    """Tests methods in APIHandler class"""

    @classmethod
    def setUpClass(cls):
        """Load mock_db before running tests."""

        co_mock_token = "abc-123"
        co_mock_domain = "https://example.com"

        with open(MOCK_RESPONSE_FILE2) as f:
            contents = json.load(f)
        mock_co_client = CodeOcean(domain=co_mock_domain, token=co_mock_token)
        mock_s3_client = MagicMock()
        cls.mock_search_all_data_assets = [
            DataAsset.from_json(json.dumps(r))
            for r in contents["search_all_data_assets"]["results"]
        ]
        cls.api_handler = APIHandler(co_client=mock_co_client)
        cls.api_handler_dry = APIHandler(co_client=mock_co_client, dryrun=True)
        cls.api_handler_s3 = APIHandler(
            co_client=mock_co_client, s3=mock_s3_client
        )

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("codeocean.data_asset.DataAssets.update_metadata")
    def test_update_tags(
        self,
        mock_update: MagicMock,
        mock_search_data_assets_iterator: MagicMock,
    ):
        """Tests update tags changes tags correctly."""
        mock_search_data_assets_iterator.return_value = (
            self.mock_search_all_data_assets
        )
        data_assets = (
            self.api_handler.co_client.data_assets.search_data_assets_iterator(
                search_params=DataAssetSearchParams(limit=1000)
            )
        )
        with self.assertLogs(level="DEBUG") as captured:
            self.api_handler.update_tags(
                tags_to_remove=["test"],
                tags_to_add=["new_tag"],
                tags_to_replace={"ECEPHYS": "ecephys"},
                data_assets=data_assets,
            )
        self.assertEqual(16, len(captured.output))
        self.assertEqual(
            {"ecephys", "655019", "raw", "new_tag"},
            mock_update.mock_calls[0].kwargs["update_params"].tags,
        )
        self.assertEqual(
            {"raw", "655019", "ecephys", "new_tag"},
            mock_update.mock_calls[2].kwargs["update_params"].tags,
        )
        self.assertEqual(
            {"new_tag"},
            mock_update.mock_calls[4].kwargs["update_params"].tags,
        )
        self.assertEqual(
            {"new_tag"},
            mock_update.mock_calls[6].kwargs["update_params"].tags,
        )
        self.assertEqual(
            {"new_tag"},
            mock_update.mock_calls[8].kwargs["update_params"].tags,
        )

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("codeocean.data_asset.DataAssets.update_metadata")
    def test_update_tags_dryrun(
        self,
        mock_update: MagicMock,
        mock_search_data_assets_iterator: MagicMock,
    ):
        """Tests update tags changes tags correctly."""
        mock_search_data_assets_iterator.return_value = (
            self.mock_search_all_data_assets
        )
        data_assets = (
            self.api_handler.co_client.data_assets.search_data_assets_iterator(
                search_params=DataAssetSearchParams(limit=1000)
            )
        )
        with self.assertLogs(level="DEBUG") as captured:
            self.api_handler_dry.update_tags(
                tags_to_remove=["test"],
                tags_to_add=["new_tag"],
                data_assets=data_assets,
            )

        mock_update.assert_not_called()
        self.assertEqual(16, len(captured.output))

    def test_bucket_prefix_exists(self):
        """Tests bucket_prefix_exists evaluation from boto response."""

        # Mock return values for list objects
        self.api_handler_s3.s3.list_objects.return_value = {}
        resp = self.api_handler_s3._bucket_prefix_exists(
            bucket="some-bucket", prefix="some-prefix"
        )
        self.assertFalse(resp)
        self.api_handler_s3.s3.list_objects.return_value = {
            "CommonPrefixes": []
        }
        resp = self.api_handler_s3._bucket_prefix_exists(
            bucket="some-bucket", prefix="some-prefix"
        )
        self.assertTrue(resp)

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    def test_find_external_assets(
        self,
        mock_search_data_assets_iterator: MagicMock,
    ):
        """Tests find_external_data_assets method"""
        mock_search_data_assets_iterator.return_value = (
            self.mock_search_all_data_assets
        )

        self.api_handler_s3.s3.list_objects.side_effect = [
            {},
            Exception("Error"),
            {"CommonPrefixes": 1},
            {"CommonPrefixes": 2},
        ]

        resp = list(self.api_handler_s3.find_external_data_assets())
        self.assertEqual(2, len(resp))

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("logging.error")
    @patch("logging.debug")
    def test_find_non_existent_external_assets(
        self,
        mock_debug: MagicMock,
        mock_log_error: MagicMock,
        mock_search_data_assets_iterator: MagicMock,
    ):
        """Tests find_nonexistent_external_data_assets method"""
        mock_search_data_assets_iterator.return_value = (
            self.mock_search_all_data_assets
        )
        self.api_handler_s3.s3.list_objects.side_effect = [
            {},
            Exception("Error"),
            {"CommonPrefixes": 1},
            {"CommonPrefixes": 2},
        ]

        resp = list(
            self.api_handler_s3.find_nonexistent_external_data_assets()
        )
        self.assertEqual(1, len(list(resp)))

        mock_debug.assert_called_once_with(
            "aind-ephys-data-dev-u5u0i5 ecephys_655019_2023-04-03_18-10-10"
            " exists? False"
        )
        mock_log_error.assert_called_once()

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("logging.debug")
    @patch("logging.info")
    def test_find_archived_data_assets_to_delete(
        self,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_search_data_assets_iterator: MagicMock,
    ):
        """Tests find_archived_data_assets_to_delete method with successful
        responses from CodeOCean"""

        mock_search_data_assets_iterator.return_value = (
            self.mock_search_all_data_assets
        )

        keep_after = datetime.datetime(year=2023, month=9, day=1)
        resp = self.api_handler.find_archived_data_assets_to_delete(
            keep_after=keep_after
        )
        self.assertEqual(6, len(resp))
        mock_log_info.assert_has_calls(
            [
                call(
                    (
                        "name: ecephys_661398_2023-03-31_17-01-09"
                        "_nwb_2023-06-01_14-50-08, type: dataset"
                    )
                ),
                call(
                    (
                        "name: ecephys_660166_2023-03-16_18-30-14_curated"
                        "_2023-03-24_17-54-16, type: dataset"
                    )
                ),
                call(
                    "name: ecephys_636766_2023-01-25_00-00-00, type: dataset"
                ),
                call(
                    (
                        "name: ecephys_636766_2023-01-23_00-00-00_sorted-ks2.5"
                        "_2023-06-01_14-48-42, type: dataset"
                    )
                ),
                call(
                    (
                        "name: ecephys_622155_2022-05-31_15-29-16_2023-06-01"
                        "_14-45-05, type: dataset"
                    )
                ),
                call(
                    (
                        "name: multiplane-ophys_438912_2019-04-17_15-19-14"
                        "_processed_2024-02-14_19-44-46, type: result"
                    )
                ),
                call("6/8 archived assets deletable"),
                call("internal: 1 assets, 535.12994798 GBs"),
                call("external: 5 assets, 0.0 GBs"),
            ]
        )

        mock_log_debug.assert_not_called()


if __name__ == "__main__":
    unittest.main()
