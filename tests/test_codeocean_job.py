"""Tests for the codeocean_job module"""

import unittest
from unittest.mock import MagicMock, call, patch

import requests
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import (
    ComputationDataAsset,
)
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest,
    Source,
    Sources,
)

from aind_codeocean_utils.codeocean_job import CodeOceanJob
from aind_codeocean_utils.models.config import (
    CaptureResultConfig,
    CodeOceanJobConfig,
    RegisterDataConfig,
    RunCapsuleConfig,
)


class TestCodeOceanJob(unittest.TestCase):
    """Tests for CodeOceanJob class"""

    @classmethod
    def setUpClass(cls):
        """Set up basic configs that can be used across all tests."""
        basic_register_data_config = RegisterDataConfig(
            asset_name="some_asset_name",
            mount="asset_mount",
            bucket="asset_bucket",
            prefix="asset_prefix",
            public=True,
            keep_on_external_storage=True,
            tags=["a", "b", "raw"],
            custom_metadata={
                "key1": "value1",
                "key2": "value2",
                "data level": "raw",
            },
            viewable_to_everyone=True,
        )
        basic_run_capsule_config = RunCapsuleConfig(
            capsule_id="123-abc",
            pipeline_id=None,
            data_assets=[
                ComputationDataAsset(id="999888", mount="some_mount"),
                {"id": "12345", "mount": "some_mount_2"},
            ],
            run_parameters=["param1", "param2"],
            pause_interval=400,
            capsule_version=3,
            timeout_seconds=10000,
        )
        basic_run_capsule_input_mount_config = RunCapsuleConfig(
            capsule_id="123-abc",
            pipeline_id=None,
            data_assets=[
                ComputationDataAsset(id="999888", mount="some_mount"),
                {"id": "12345", "mount": "some_mount_2"},
            ],
            input_data_mount="custom-mount",
            run_parameters=["param1", "param2"],
            pause_interval=400,
            capsule_version=3,
            timeout_seconds=10000,
        )
        basic_run_capsule_one_asset_config = RunCapsuleConfig(
            capsule_id=None,
            pipeline_id="123-abc",
            data_assets=[
                {"id": "12345", "mount": "some_mount_2"},
            ],
            run_parameters=["param1", "param2"],
            pause_interval=400,
            capsule_version=3,
            timeout_seconds=10000,
        )
        basic_capture_result_config = CaptureResultConfig(
            process_name="some_process",
            mount="some_mount",
            asset_name="some_asset_name",
            tags=["x", "y"],
            custom_metadata={
                "key1": "value1",
                "key2": "value2",
            },
            viewable_to_everyone=True,
        )
        none_vals_capture_result_config = CaptureResultConfig(
            process_name="some_process",
            mount=None,
            asset_name=None,
            tags=["x", "y", "a", "b", "raw"],
            custom_metadata={
                "key1": "value1",
                "key2": "value2",
                "data level": "raw",
            },
            viewable_to_everyone=True,
        )
        none_vals_capture_result_config_w_asset_name = CaptureResultConfig(
            process_name="some_process",
            mount=None,
            asset_name="some_asset_name",
            tags=["x", "y", "a", "b", "raw"],
            custom_metadata={
                "key1": "value1",
                "key2": "value2",
                "data level": "raw",
            },
            viewable_to_everyone=True,
        )

        co_domain = "http://codeocean.acme.org"
        co_token = "co_api_token_1234"
        cls.co_client = CodeOceanClient(domain=co_domain, token=co_token)
        cls.basic_codeocean_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            run_capsule_config=basic_run_capsule_config,
            capture_result_config=basic_capture_result_config,
        )
        cls.basic_input_mount_codeocean_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            run_capsule_config=basic_run_capsule_input_mount_config,
        )
        cls.none_vals_codeocean_job_config = CodeOceanJobConfig(
            register_config=basic_register_data_config,
            run_capsule_config=basic_run_capsule_config,
            capture_result_config=none_vals_capture_result_config,
        )
        cls.no_reg_codeocean_job_config_no_asset_name = CodeOceanJobConfig(
            register_config=None,
            run_capsule_config=basic_run_capsule_config,
            capture_result_config=none_vals_capture_result_config,
        )
        cls.no_reg_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            run_capsule_config=basic_run_capsule_config,
            capture_result_config=none_vals_capture_result_config_w_asset_name,
        )
        cls.one_asset_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            run_capsule_config=basic_run_capsule_one_asset_config,
            capture_result_config=basic_capture_result_config,
        )
        cls.none_vals_codeocean_job_config = CodeOceanJobConfig(
            register_config=None,
            run_capsule_config=basic_run_capsule_one_asset_config,
            capture_result_config=none_vals_capture_result_config,
        )

    def test_class_constructor(self):
        """Tests constructor"""
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        self.assertEqual(self.co_client, codeocean_job.co_client)
        self.assertEqual(
            self.basic_codeocean_job_config, codeocean_job.job_config
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
        response = codeocean_job._wait_for_data_availability(
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
        response = codeocean_job._wait_for_data_availability(
            data_asset_id="123"
        )
        self.assertEqual(500, response.status_code)
        self.assertEqual(some_response.json, response.json)
        self.assertEqual(32, mock_sleep.call_count)

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_computation")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.run_capsule")
    def test_run_capsule_check_not_found(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _run_capsule with data asset not found response"""
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        some_response = requests.Response()
        some_response.status_code = 404
        some_response.json = {"message: Not Found"}
        mock_get_data_asset.return_value = some_response
        with self.assertRaises(FileNotFoundError) as e:
            codeocean_job._run_capsule(
                run_capsule_config=(
                    self.basic_codeocean_job_config.run_capsule_config
                ),
                input_data_assets=[
                    ComputationDataAsset(id="999888", mount="some_mount")
                ],
            )

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
    def test_run_capsule_check_server_failed(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _run_capsule with a server error response"""
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        some_response = requests.Response()
        some_response.status_code = 500
        some_response.json = {"Something went wrong"}
        mock_get_data_asset.return_value = some_response
        with self.assertRaises(ConnectionError) as e:
            codeocean_job._run_capsule(
                run_capsule_config=(
                    self.basic_codeocean_job_config.run_capsule_config
                )
            )

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
    def test_run_capsule_check_passed(
        self,
        mock_run_capsule: MagicMock,
        mock_get_computation: MagicMock,
        mock_get_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _run_capsule with successful responses from code ocean"""
        some_get_data_asset_response = requests.Response()
        some_get_data_asset_response.status_code = 200
        some_get_data_asset_response.json = {
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

        response = codeocean_job._run_capsule(
            run_capsule_config=codeocean_job.job_config.run_capsule_config
        )
        mock_sleep.assert_called_once_with(400)
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

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_register_data_and_update_permissions(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _register_data_and_update_permissions"""
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
        actual_response = codeocean_job._register_data_and_update_permissions(
            register_data_config=codeocean_job.job_config.register_config
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_register_data_and_update_permissions_failure(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _register_data_and_update_permissions with a fail response"""
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
            codeocean_job._register_data_and_update_permissions(
                register_data_config=codeocean_job.job_config.register_config
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
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_capture_result(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _capture_result"""
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
        actual_response = codeocean_job._capture_result(
            capture_result_config=(
                codeocean_job.job_config.capture_result_config
            ),
            computation_id="124fq",
            input_data_asset_name=None,
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_capture_result_additional_tags_and_metadata(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _capture_result with additional tags and metadata"""
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

        # check that duplicated tags and metadata are not added
        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        capture_tags = codeocean_job.job_config.capture_result_config.tags
        capture_metadata = (
            codeocean_job.job_config.capture_result_config.custom_metadata
        )
        capture_metadata_input = capture_metadata.copy()
        capture_metadata_input.update({"data level": "raw"})
        capture_metadata_output = capture_metadata.copy()
        capture_metadata_output.update({"data level": "derived"})

        codeocean_job._capture_result(
            capture_result_config=(
                codeocean_job.job_config.capture_result_config
            ),
            computation_id="124fq",
            input_data_asset_name=None,
            additional_tags=capture_tags + ["raw"],
            additional_custom_metadata=capture_metadata_input,
        )
        mock_create_data_asset.assert_has_calls(
            [
                call(
                    CreateDataAssetRequest(
                        name="some_asset_name",
                        tags=capture_tags + ["derived"],
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

        codeocean_job._capture_result(
            capture_result_config=(
                codeocean_job.job_config.capture_result_config
            ),
            computation_id="124fq",
            input_data_asset_name=None,
            additional_tags=["extra_tag1", "extra_tag2"],
            additional_custom_metadata={"extra_key": "extra_value"},
        )
        mock_create_data_asset.assert_has_calls(
            [
                call(
                    CreateDataAssetRequest(
                        name="some_asset_name",
                        tags=["x", "y", "extra_tag1", "extra_tag2"],
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
                        custom_metadata={
                            "key1": "value1",
                            "key2": "value2",
                            "extra_key": "extra_value",
                        },
                    )
                )
            ]
        )

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_capture_result_none_vals(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _capture_result with asset_name and mount set to None"""
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
            job_config=self.none_vals_codeocean_job_config,
        )
        with self.assertRaises(AssertionError) as e:
            codeocean_job._capture_result(
                capture_result_config=(
                    codeocean_job.job_config.capture_result_config
                ),
                computation_id="124fq",
                input_data_asset_name=None,
            )
        self.assertEqual(
            (
                "AssertionError('Either asset_name or input_data_asset_name"
                " must be provided')"
            ),
            repr(e.exception),
        )

        actual_response = codeocean_job._capture_result(
            capture_result_config=(
                codeocean_job.job_config.capture_result_config
            ),
            computation_id="124fq",
            input_data_asset_name="some_input_data_asset_name",
        )
        self.assertEqual(some_create_data_asset_response, actual_response)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.create_data_asset")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.update_permissions")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_capture_result_registration_failed(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _capture_result with failed registration step"""
        some_create_data_asset_response = requests.Response()
        some_create_data_asset_response.status_code = 500
        some_create_data_asset_response.json = lambda: (
            {"messsage": "Something went wrong!"}
        )
        mock_create_data_asset.return_value = some_create_data_asset_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(KeyError) as e:
            codeocean_job._capture_result(
                capture_result_config=(
                    codeocean_job.job_config.capture_result_config
                ),
                computation_id="124fq",
                input_data_asset_name=None,
            )
        self.assertEqual(
            (
                'KeyError("Something went wrong registering some_asset_name.'
                " Response Status Code: 500. Response Message:"
                " {'messsage': 'Something went wrong!'}\")"
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
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._wait_for_data_availability"
    )
    def test_capture_result_wait_for_data_failure(
        self,
        mock_wait_for_data_availability: MagicMock,
        mock_update_permissions: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_sleep: MagicMock,
    ):
        """Tests _capture_result with wait_for_data failure"""
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

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        with self.assertRaises(FileNotFoundError) as e:
            codeocean_job._capture_result(
                capture_result_config=(
                    codeocean_job.job_config.capture_result_config
                ),
                computation_id="124fq",
                input_data_asset_name=None,
            )

        self.assertEqual(
            "FileNotFoundError('Unable to find: abc-123')", repr(e.exception)
        )
        mock_sleep.assert_not_called()
        mock_update_permissions.assert_not_called()

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._capture_result")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._register_data_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._run_capsule")
    def test_run_job(
        self,
        mock_run_capsule: MagicMock,
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
        register_mount = self.basic_codeocean_job_config.register_config.mount
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
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_called_once_with(
            self.basic_codeocean_job_config.register_config
        )

        mock_run_capsule.assert_called_once_with(
            self.basic_codeocean_job_config.run_capsule_config,
            input_data_assets=[
                ComputationDataAsset(
                    id=fake_register_id,
                    mount=register_mount,
                )
            ],
        )

        # the run_capsule will propagate the additional_tags and
        # additional_custom_metadata to the _capture_result method
        mock_capture_result.assert_called_once_with(
            computation_id=fake_computation_id,
            input_data_asset_name="some_asset_name",
            additional_tags=["a", "b", "raw"],
            additional_custom_metadata={
                "key1": "value1",
                "key2": "value2",
                "data level": "raw",
            },
            capture_result_config=(
                self.basic_codeocean_job_config.capture_result_config
            ),
        )

    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._register_data_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._run_capsule")
    def test_run_job_input_data(
        self,
        mock_run_capsule: MagicMock,
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
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.basic_input_mount_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_called_once_with(
            self.basic_input_mount_codeocean_job_config.register_config
        )

        run_capsule_config = (
            self.basic_input_mount_codeocean_job_config.run_capsule_config
        )
        mock_run_capsule.assert_called_once_with(
            self.basic_input_mount_codeocean_job_config.run_capsule_config,
            input_data_assets=[
                ComputationDataAsset(
                    id=fake_register_id,
                    mount=run_capsule_config.input_data_mount,
                )
            ],
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._capture_result")
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._run_capsule")
    def test_run_job_no_registration(
        self,
        mock_run_capsule: MagicMock,
        mock_capture_result: MagicMock,
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
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.no_reg_codeocean_job_config,
        )
        codeocean_job.run_job()

        mock_capture_result.assert_called_once_with(
            computation_id=fake_computation_id,
            input_data_asset_name="some_asset_name",
            additional_tags=[],
            additional_custom_metadata={},
            capture_result_config=(
                self.no_reg_codeocean_job_config.capture_result_config
            ),
        )

        with self.assertRaises(AssertionError) as e:
            codeocean_job = CodeOceanJob(
                co_client=self.co_client,
                job_config=self.no_reg_codeocean_job_config_no_asset_name,
            )
            codeocean_job.run_job()
        self.assertEqual(
            (
                "AssertionError('If a data asset was not registered and the "
                "job used more than one data asset, then the "
                "input_data_asset_name must be provided.')"
            ),
            repr(e.exception),
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._capture_result")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._register_data_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._run_capsule")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_run_job_one_data_asset(
        self,
        mock_get_data_asset: MagicMock,
        mock_run_capsule: MagicMock,
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
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.one_asset_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_not_called()
        mock_run_capsule.assert_called_once_with(
            self.one_asset_codeocean_job_config.run_capsule_config,
            input_data_assets=None,
        )
        # the run_capsule will propagate the additional_tags and
        # additional_custom_metadata to the _capture_result method
        mock_capture_result.assert_called_once_with(
            computation_id=fake_computation_id,
            input_data_asset_name="some_asset_name",
            additional_tags=["ecephys", "raw"],
            additional_custom_metadata=None,
            capture_result_config=(
                self.one_asset_codeocean_job_config.capture_result_config
            ),
        )

    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._capture_result")
    @patch(
        "aind_codeocean_utils.codeocean_job.CodeOceanJob"
        "._register_data_and_update_permissions"
    )
    @patch("aind_codeocean_utils.codeocean_job.CodeOceanJob._run_capsule")
    @patch("aind_codeocean_api.codeocean.CodeOceanClient.get_data_asset")
    def test_run_job_one_data_asset_none_capture_result_config(
        self,
        mock_get_data_asset: MagicMock,
        mock_run_capsule: MagicMock,
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
        mock_run_capsule.return_value = some_run_response

        codeocean_job = CodeOceanJob(
            co_client=self.co_client,
            job_config=self.none_vals_codeocean_job_config,
        )
        codeocean_job.run_job()
        mock_register_data.assert_not_called()
        mock_run_capsule.assert_called_once_with(
            self.none_vals_codeocean_job_config.run_capsule_config,
            input_data_assets=None,
        )
        # the run_capsule will propagate the additional_tags and
        # additional_custom_metadata to the _capture_result method
        mock_capture_result.assert_called_once_with(
            computation_id=fake_computation_id,
            input_data_asset_name="ecephys_632269_2022-10-10_16-13-22",
            additional_tags=["ecephys", "raw"],
            additional_custom_metadata=None,
            capture_result_config=(
                self.none_vals_codeocean_job_config.capture_result_config
            ),
        )


if __name__ == "__main__":
    unittest.main()
