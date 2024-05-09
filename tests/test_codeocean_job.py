"""Tests for the codeocean_job module"""

import unittest
from unittest.mock import MagicMock, call, patch

import requests
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import (
    ComputationDataAsset,
    RunCapsuleRequest,
)
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest,
    Source,
    Sources,
)

from aind_codeocean_utils.codeocean_job import (
    CodeOceanJob,
    CodeOceanJobConfig,
    ProcessConfig,
    CaptureConfig,
    build_processed_data_asset_name,
)


class TestCodeOceanJob(unittest.TestCase):
    """Tests for CodeOceanJob class"""

    @classmethod
    def setUpClass(cls):
        """Set up basic configs that can be used across all tests."""
        basic_register_data_config = CreateDataAssetRequest(
            name="platform_subject_date_time",
            mount="deleteme",
            source=Source(
                aws=Sources.AWS(
                    bucket="asset_bucket",
                    prefix="asset_prefix",
                    keep_on_external_storage=True,
                )
            ),
            tags=sorted(["raw", "a", "b"]),
            custom_metadata={
                "key1": "value1",
                "key2": "value2",
                "data level": "raw",
            },
        )
        basic_process_config = ProcessConfig(
            request=RunCapsuleRequest(
                capsule_id="123-abc",
                pipeline_id=None,
                data_assets=[
                    dict(id="999888", mount="some_mount"),
                    dict(id="12345", mount="some_mount_2"),
                ],
                parameters=["param1", "param2"],
            )
        )
        basic_process_config_no_assets = ProcessConfig(
            request=RunCapsuleRequest(
                capsule_id="123-abc",
                pipeline_id=None,
                data_assets=None,
                parameters=["param1", "param2"],
            ),
            input_data_asset_mount="custom-mount",
        )
        basic_process_data_input_mount_config = ProcessConfig(
            request=RunCapsuleRequest(
                capsule_id="123-abc",
                pipeline_id=None,
                data_assets=[
                    ComputationDataAsset(id="999888", mount="some_mount"),
                    ComputationDataAsset(id="12345", mount="some_mount_2"),
                ],
                parameters=["param1", "param2"],
                version=3,
            ),
            input_data_asset_mount="custom-mount",
            poll_interval_seconds=400,
            timeout_seconds=10000,
        )
        basic_process_data_one_asset_config = ProcessConfig(
            request=RunCapsuleRequest(
                capsule_id=None,
                pipeline_id="123-abc",
                data_assets=[
                    dict(id="12345", mount="some_mount_2"),
                ],
                parameters=["param1", "param2"],
                version=3,
            ),
            poll_interval_seconds=400,
            timeout_seconds=10000,
        )
        basic_capture_config = CaptureConfig(
            process_name="some_process",
            request=CreateDataAssetRequest(
                mount="some_mount",
                name="some_asset_name",
                tags=["x", "y"],
                custom_metadata={
                    "key1": "value1",
                    "key2": "value2",
                },
            ),
        )
        basic_capture_config_no_request = CaptureConfig(
            process_name="some_process", request=None
        )
        none_vals_capture_config = CaptureConfig(
            process_name="some_process",
            request=CreateDataAssetRequest(
                mount=None,
                name=None,
                tags=["x", "y", "a", "b", "raw"],
                custom_metadata={
                    "key1": "value1",
                    "key2": "value2",
                    "data level": "raw",
                },
            ),
        )
        none_vals_capture_config_w_asset_name = CaptureConfig(
            process_name="some_process",
            request=CreateDataAssetRequest(
                mount=None,
                name="some_asset_name",
                tags=["x", "y", "a", "b", "raw"],
                custom_metadata={
                    "key1": "value1",
                    "key2": "value2",
                    "data level": "raw",
                },
            ),
        )

        co_domain = "http://codeocean.acme.org"
        co_token = "co_api_token_1234"
        cls.co_client = CodeOceanClient(domain=co_domain, token=co_token)
        cls.basic_codeocean_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            process_config=basic_process_config,
            capture_config=basic_capture_config,
        )
        cls.basic_codeocean_job_config_no_assets = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            process_config=basic_process_config_no_assets,
            capture_config=basic_capture_config,
        )
        cls.basic_input_mount_codeocean_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            process_config=basic_process_data_input_mount_config,
        )
        cls.no_capture_request_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            process_config=basic_process_config_no_assets,
            capture_config=basic_capture_config_no_request,
        )
        cls.no_reg_codeocean_job_config_no_asset_name = CodeOceanJobConfig(
            register_config=None,
            process_config=basic_process_config,
            capture_config=none_vals_capture_config,
        )
        cls.no_reg_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            process_config=basic_process_config,
            capture_config=none_vals_capture_config_w_asset_name,
        )
        cls.one_asset_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            process_config=basic_process_data_one_asset_config,
            capture_config=basic_capture_config_no_request,
        )
        cls.multi_asset_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            process_config=basic_process_config,
            capture_config=none_vals_capture_config,
        )
        cls.no_process_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            process_config=None,
            capture_config=none_vals_capture_config,
        )

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_wait_for_data_availability_success(
        self, mock_get_data_asset: MagicMock, mock_sleep: MagicMock
    ):
        """Tests _wait_for_data_availability"""
        some_response = requests.Response()
        some_response.status_code = 200
        fake_data_asset_id = "abc-123"
        some_response.json = {
            "created": 1666322134,
            "description": "",
            "files": 1364,
            "id": fake_data_asset_id,
            "last_used": 0,
            "name": "ecephys_632269_2022-10-10_16-13-22",
            "size": 3632927966,
            "state": "ready",
            "tags": ["ecephys", "raw"],
            "type": "dataset",
        }
        mock_get_data_asset.return_value = some_response
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        response = codeocean_job.api_handler.wait_for_data_availability(
            data_asset_id=fake_data_asset_id
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(some_response.json, response.json)
        mock_sleep.assert_called_once_with(10)

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_wait_for_data_availability_timeout(
        self, mock_get_data_asset: MagicMock, mock_sleep: MagicMock
    ):
        """Tests _wait_for_data_availability with timeout"""
        some_response = requests.Response()
        some_response.status_code = 500
        some_response.json = {"Something went wrong!"}
        mock_get_data_asset.return_value = some_response
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        response = codeocean_job.api_handler.wait_for_data_availability(
            data_asset_id="123"
        )
        self.assertEqual(500, response.status_code)
        self.assertEqual(some_response.json, response.json)
        self.assertEqual(32, mock_sleep.call_count)

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_computation")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_process_data_check_not_found(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _process_data with data asset not found response"""
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        some_response = requests.Response()
        some_response.status_code = 404
        some_response.json = {"message: Not Found"}
        mock_get_data_asset.return_value = some_response

        codeocean_job.process_config.request.data_assets = [
            ComputationDataAsset(id="999888", mount="some_mount")
        ]

        with self.assertRaises(FileNotFoundError) as e:
            codeocean_job.process_data()

        self.assertEqual(
            "FileNotFoundError('Unable to find: 999888')", repr(e.exception)
        )
        mock_run_capsule.assert_not_called()
        mock_get_computation.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_computation")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_process_data_check_server_failed(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _process_data with a server error response"""
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        some_response = requests.Response()
        some_response.status_code = 500
        some_response.json = {"Something went wrong"}
        mock_get_data_asset.return_value = some_response
        with self.assertRaises(ConnectionError) as e:
            codeocean_job.process_data()

        self.assertEqual(
            "ConnectionError('There was an issue retrieving: 999888')",
            repr(e.exception),
        )
        mock_run_capsule.assert_not_called()
        mock_get_computation.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_computation")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_process_data_check_passed(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _process_data with successful responses from code ocean"""
        some_get_data_asset_response = requests.Response()
        some_get_data_asset_response.status_code = 200
        some_get_data_asset_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": "999888",
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )

        mock_get_data_asset.return_value = some_get_data_asset_response

        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_run_capsule.return_value = some_run_response

        some_comp_response = requests.Response()
        some_comp_response.status_code = 200
        some_comp_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )
        mock_get_computation.return_value = some_comp_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )

        response = codeocean_job.process_data(
            register_data_response=some_get_data_asset_response
        )
        mock_sleep.assert_called_once_with(300)
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "created": 1646943238,
                "has_results": False,
                "id": "comp-abc-123",
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            },
            response.json(),
        )
        codeocean_job_no_assets = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config_no_assets,
        )

        response = codeocean_job_no_assets.process_data(
            register_data_response=some_get_data_asset_response
        )

        # test failed response ID
        some_run_response = requests.Response()
        some_run_response.status_code = 200
        some_run_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": None,
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(KeyError) as e:
            codeocean_job.process_data(
                register_data_response=some_get_data_asset_response
            )

        assert "Something went wrong running the capsule or pipeline." in repr(
            e.exception
        )

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler."
        "APIHandler.wait_for_data_availability"
    )
    def test_create_data_asset_and_update_permissions(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _create_data_asset_and_update_permissions"""
        fake_data_asset_id = "abc-123"

        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 200
        some_wait_for_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        some_update_permissions_response = requests.Response()
        some_update_permissions_response.status_code = 204
        mock_update_permissions.return_value = some_update_permissions_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        actual_response = (
            codeocean_job.api_handler.create_data_asset_and_update_permissions(
                request=codeocean_job.register_config,
                assets_viewable_to_everyone=(
                    codeocean_job.assets_viewable_to_everyone
                ),
            )
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "wait_for_data_availability"
    )
    def test_create_data_asset_and_update_permissions_failure(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """
        Tests _create_data_asset_and_update_permissions with
        a fail response
        """
        fake_data_asset_id = "abc-123"

        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 500
        some_wait_for_data_response.json = {"Something went wrong!"}
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(FileNotFoundError) as e:
            codeocean_job.api_handler.create_data_asset_and_update_permissions(
                request=codeocean_job.register_config,
                assets_viewable_to_everyone=(
                    codeocean_job.assets_viewable_to_everyone
                ),
            )
        self.assertEqual(
            "FileNotFoundError('Unable to find: abc-123')", repr(e.exception)
        )
        mock_update_permissions.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "wait_for_data_availability"
    )
    def test_capture_result(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests capture_results"""
        fake_data_asset_id = "abc-123"

        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 200
        some_wait_for_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        some_update_permissions_response = requests.Response()
        some_update_permissions_response.status_code = 204
        mock_update_permissions.return_value = some_update_permissions_response

        some_process_response = requests.Response()
        some_process_response.status_code = 200
        some_process_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": "fake_id",
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        actual_response = codeocean_job.capture_result(
            process_response=some_process_response
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

        # test no capture request
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.no_capture_request_job_config,
        )
        actual_response = codeocean_job.capture_result(
            process_response=some_process_response
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "wait_for_data_availability"
    )
    def test_capture_result_additional_tags_and_metadata(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests capture_results with additional tags and metadata"""
        fake_data_asset_id = "abc-123"
        #
        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 200
        some_wait_for_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        some_update_permissions_response = requests.Response()
        some_update_permissions_response.status_code = 204
        mock_update_permissions.return_value = some_update_permissions_response

        some_process_response = requests.Response()
        some_process_response.status_code = 200
        some_process_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": "124fq",
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )

        # check that duplicated tags and metadata are not added
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        capture_tags = codeocean_job.capture_config.request.tags.copy()
        capture_metadata = codeocean_job.capture_config.request.custom_metadata

        capture_metadata_input = capture_metadata.copy()
        capture_metadata_input.update({"data level": "raw"})
        capture_metadata_output = capture_metadata.copy()
        capture_metadata_output.update({"data level": "derived"})

        codeocean_job.capture_result(process_response=some_process_response)
        mock_create_data_asset.assert_has_calls(
            [
                call(
                    CreateDataAssetRequest(
                        name="some_asset_name",
                        tags=sorted(list(set(capture_tags + ["derived"]))),
                        mount="some_mount",
                        description=None,
                        source=Source(
                            aws=None,
                            gcp=None,
                            computation=Sources.Computation(
                                id="124fq", path=None
                            ),
                        ),
                        target=None,
                        custom_metadata=capture_metadata_output,
                    )
                )
            ]
        )
        mock_sleep.assert_not_called()

        some_get_data_asset_response = requests.Response()
        some_get_data_asset_response.status_code = 200
        some_get_data_asset_response.json = lambda: (
            {
                "name": "some_custom_asset_name",
                "tags": ["my-custom-input-tag"],
                "custom_metadata": {"data level": "raw", "key1": "value1"},
            }
        )
        mock_get_data_asset.return_value = some_get_data_asset_response

        # test inheriting tags and metadata from input data asset
        mock_create_data_asset.reset_mock()
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.one_asset_codeocean_job_config,
        )
        codeocean_job.capture_result(process_response=some_process_response)

        captured_asset_name = build_processed_data_asset_name(
            "some_custom_asset_name", codeocean_job.capture_config.process_name
        )
        mock_create_data_asset.assert_has_calls(
            [
                call(
                    CreateDataAssetRequest(
                        name=captured_asset_name,
                        tags=["derived", "my-custom-input-tag"],
                        mount=captured_asset_name,
                        description=None,
                        source=Source(
                            aws=None,
                            gcp=None,
                            computation=Sources.Computation(
                                id="124fq", path=None
                            ),
                        ),
                        target=None,
                        custom_metadata={
                            "data level": "derived",
                            "key1": "value1",
                        },
                    )
                )
            ]
        )

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "wait_for_data_availability"
    )
    def test_capture_result_none_vals(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests capture_results with asset_name and mount set to None"""
        fake_data_asset_id = "abc-123"

        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 200
        some_wait_for_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        some_update_permissions_response = requests.Response()
        some_update_permissions_response.status_code = 204
        mock_update_permissions.return_value = some_update_permissions_response

        some_process_response = requests.Response()
        some_process_response.status_code = 200
        some_process_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": "124fq",
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )

        # Test getting asset name from attached data asset
        some_get_data_asset_response = requests.Response()
        some_get_data_asset_response.status_code = 200
        some_get_data_asset_response.json = lambda: (
            {
                "name": "some_input_data_asset_name",
            }
        )
        mock_get_data_asset.return_value = some_get_data_asset_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.one_asset_codeocean_job_config,
        )

        codeocean_job.capture_result(process_response=some_process_response)

        codeocean_job.capture_config.input_data_asset_name = (
            "some_input_data_asset_name"
        )
        actual_response = codeocean_job.capture_result(
            process_response=some_process_response
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

        # Test exception when multiple input data assets is not provided
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.multi_asset_codeocean_job_config,
        )
        with self.assertRaises(AssertionError) as e:
            codeocean_job.capture_result(
                process_response=some_process_response
            )
        self.assertEqual(
            (
                "AssertionError('Data asset name not provided and multiple "
                "data assets were provided in the process configuration')"
            ),
            repr(e.exception),
        )
        # Test exception when no input data assets is provided
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.no_process_codeocean_job_config,
        )
        with self.assertRaises(AssertionError) as e:
            codeocean_job.capture_result(
                process_response=some_process_response
            )
        self.assertEqual(
            ("AssertionError('Data asset name not provided')"),
            repr(e.exception),
        )

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "wait_for_data_availability"
    )
    def test_capture_result_registration_failed(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests capture_results with failed registration step"""
        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 500
        some_create_data_asset_response.json = lambda: (
            {"messsage": "Something went wrong!"}
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_process_response = requests.Response()
        some_process_response.status_code = 200
        some_process_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": "124fq",
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(KeyError) as e:
            codeocean_job.capture_result(
                process_response=some_process_response
            )

        self.assertEqual(
            (
                'KeyError("Something went wrong registering '
                "'some_asset_name'. Response Status Code: 500. "
                "Response Message: {'messsage': 'Something went wrong!'}\")"
            ),
            repr(e.exception),
        )
        mock_sleep.assert_not_called()
        mock_wait_for_data_availability.assert_not_called()
        mock_update_permissions.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler"
        ".wait_for_data_availability"
    )
    def test_capture_result_wait_for_data_failure(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests capture_results with wait_for_data failure"""
        fake_data_asset_id = "abc-123"

        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 200
        some_create_data_asset_response.json = lambda: (
            {
                "created": 1641420832,
                "description": "",
                "files": 0,
                "id": fake_data_asset_id,
                "lastUsed": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "sizeInBytes": 0,
                "state": "DATA_ASSET_STATE_DRAFT",
                "tags": ["ecephys", "raw"],
                "type": "DATA_ASSET_TYPE_DATASET",
            }
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        some_wait_for_data_response = requests.Response()
        some_wait_for_data_response.status_code = 500
        some_wait_for_data_response.json = lambda: (
            {"message": "Something went wrong!"}
        )
        mock_wait_for_data_availability.return_value = (
            some_wait_for_data_response
        )

        some_process_response = requests.Response()
        some_process_response.status_code = 200
        some_process_response.json = lambda: (
            {
                "created": 1668125314,
                "end_status": "succeeded",
                "has_results": False,
                "id": "124fq",
                "name": "Run With Parameters 8125314",
                "parameters": [
                    {"name": "", "value": '{"p_1": {"p1_1": "some_path"}}'}
                ],
                "run_time": 8,
                "state": "completed",
            }
        )

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(FileNotFoundError) as e:
            codeocean_job.capture_result(
                process_response=some_process_response
            )

        self.assertEqual(
            "FileNotFoundError('Unable to find: abc-123')", repr(e.exception)
        )
        mock_sleep.assert_not_called()
        mock_update_permissions.assert_not_called()

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.capture_result")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "create_data_asset_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.process_data")
    def test_run_job(
        self,
        mock_process_data: MagicMock,
        mock_register_data: MagicMock,
        mock_capture_result: MagicMock,
    ):
        """Tests run_job method"""
        some_register_response = requests.Response()
        some_register_response.status_code = 200
        fake_register_id = "12345"
        custom_metadata = (
            self.basic_codeocean_job_config.register_config.custom_metadata
        )
        some_register_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_register_id,
                "last_used": 0,
                "name": "some_asset_name",
                "state": "draft",
                "custom_metadata": custom_metadata,
                "tags": self.basic_codeocean_job_config.register_config.tags,
                "type": "dataset",
            }
        )
        mock_register_data.return_value = some_register_response

        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_process_data.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        codeocean_job.run_job()
        request = self.basic_codeocean_job_config.register_config
        request.tags = sorted(request.tags + ["platform", "subject"])
        request.custom_metadata.update(
            {"experiment type": "platform", "subject id": "subject"}
        )
        mock_register_data.assert_called_once_with(
            request=request,
            assets_viewable_to_everyone=(
                codeocean_job.assets_viewable_to_everyone
            ),
        )

        mock_process_data.assert_called_once_with(
            register_data_response=some_register_response
        )

        # the process_data will propagate the additional_tags and
        # additional_custom_metadata to the capture_results method
        mock_capture_result.assert_called_once_with(
            process_response=some_run_response
        )

    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "create_data_asset_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.process_data")
    def test_run_job_input_data(
        self,
        mock_process_data: MagicMock,
        mock_register_data: MagicMock,
    ):
        """Tests run_job method"""
        some_register_response = requests.Response()
        some_register_response.status_code = 200
        fake_register_id = "12345"
        custom_metadata = (
            self.basic_codeocean_job_config.register_config.custom_metadata
        )
        some_register_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_register_id,
                "last_used": 0,
                "name": "some_asset_name",
                "state": "draft",
                "custom_metadata": custom_metadata,
                "tags": self.basic_codeocean_job_config.register_config.tags,
                "type": "dataset",
            }
        )
        mock_register_data.return_value = some_register_response

        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_process_data.return_value = some_run_response

        self.basic_input_mount_codeocean_job_config.\
            add_subject_and_platform_metadata = False
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_input_mount_codeocean_job_config,
        )

        codeocean_job.run_job()
        mock_register_data.assert_called_once_with(
            request=(
                self.basic_input_mount_codeocean_job_config.register_config
            ),
            assets_viewable_to_everyone=(
                codeocean_job.assets_viewable_to_everyone
            ),
        )

        mock_process_data.assert_called_once_with(
            register_data_response=some_register_response
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.register_data")
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.process_data")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "create_data_asset_and_update_permissions"
    )
    def test_run_job_no_registration(
        self,
        mock_create_data_asset: MagicMock,
        mock_process_data: MagicMock,
        mock_register_data: MagicMock,
    ):
        """Tests run_job method with Optional register_data set to None"""
        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_process_data.return_value = some_run_response

        some_register_response = requests.Response()
        some_register_response.status_code = 200
        fake_register_id = "12345"
        custom_metadata = (
            self.basic_codeocean_job_config.register_config.custom_metadata
        )
        some_register_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_register_id,
                "last_used": 0,
                "name": "some_asset_name",
                "state": "draft",
                "custom_metadata": custom_metadata,
                "tags": self.basic_codeocean_job_config.register_config.tags,
                "type": "dataset",
            }
        )
        mock_register_data.return_value = some_register_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.no_reg_codeocean_job_config,
        )
        codeocean_job.run_job()

        mock_register_data.assert_not_called()

        with self.assertRaises(AssertionError) as e:
            codeocean_job = CodeOceanJob(
                co_client=self.co_client,
                job_config=self.no_reg_codeocean_job_config_no_asset_name,
            )
            codeocean_job.run_job()

        self.assertEqual(
            (
                "AssertionError('Data asset name not provided and multiple "
                "data assets were provided in the process configuration')"
            ),
            repr(e.exception),
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.capture_result")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "create_data_asset_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.process_data")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_run_job_one_data_asset(
        self,
        mock_get_data_asset: MagicMock,
        mock_process_data: MagicMock,
        mock_register_data: MagicMock,
        mock_capture_result: MagicMock,
    ):
        """Tests run_job method with only one data asset attached"""

        some_get_data_response = requests.Response()
        some_get_data_response.status_code = 200
        fake_data_asset_id = "12345"
        some_get_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_get_data_asset.return_value = some_get_data_response

        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_process_data.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.one_asset_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_not_called()
        mock_process_data.assert_called_once_with(register_data_response=None)
        # the process_data will propagate the additional_tags and
        # additional_custom_metadata to the capture_results method
        mock_capture_result.assert_called_once_with(
            process_response=some_run_response
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.capture_result")
    @patch(
        "aind_codeocean_utils.api_handler.APIHandler."
        "create_data_asset_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob.process_data")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_run_job_one_data_asset_none_capture_config(
        self,
        mock_get_data_asset: MagicMock,
        mock_process_data: MagicMock,
        mock_register_data: MagicMock,
        mock_capture_result: MagicMock,
    ):
        """Tests run_job without data asset name in capture result config"""

        some_get_data_response = requests.Response()
        some_get_data_response.status_code = 200
        fake_data_asset_id = "12345"
        some_get_data_response.json = lambda: (
            {
                "created": 1666322134,
                "description": "",
                "files": 1364,
                "id": fake_data_asset_id,
                "last_used": 0,
                "name": "ecephys_632269_2022-10-10_16-13-22",
                "size": 3632927966,
                "state": "ready",
                "tags": ["ecephys", "raw"],
                "type": "dataset",
            }
        )
        mock_get_data_asset.return_value = some_get_data_response

        some_run_response = requests.Response()
        some_run_response.status_code = 200
        fake_computation_id = "comp-abc-123"
        some_run_response.json = lambda: (
            {
                "created": 1646943238,
                "has_results": False,
                "id": fake_computation_id,
                "name": "Run 6943238",
                "run_time": 1,
                "state": "initializing",
            }
        )
        mock_process_data.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.one_asset_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_not_called()
        mock_process_data.assert_called_once_with(register_data_response=None)
        # the process_data will propagate the additional_tags and
        # additional_custom_metadata to the capture_results method
        mock_capture_result.assert_called_once_with(
            process_response=some_run_response
        )

    def test_build_processed_data_asset_name(self):
        """Tests build_processed_data_asset_name function"""
        input_data_asset_name = "ecephys_00000_2022-10-10_16-13-22"
        process_name = "test-process"
        processed_asset_name = build_processed_data_asset_name(
            input_data_asset_name, process_name
        )
        assert processed_asset_name.startswith(
            f"{input_data_asset_name}_{process_name}_"
        )


if __name__ == "__main__":
    unittest.main()
