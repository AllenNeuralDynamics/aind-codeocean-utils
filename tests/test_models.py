"""Tests for the models package"""

import unittest

from aind_codeocean_api.models.computations_requests import (
    ComputationDataAsset,
)

from aind_codeocean_utils.models.config import (
    CaptureResultConfig,
    CodeOceanJobConfig,
    RegisterDataConfig,
    RunCapsuleConfig,
)
from tests import PYD_VERSION


class TestRegisterDataConfig(unittest.TestCase):
    """Tests for RegisterDataConfig class"""

    def test_success_all_fields(self):
        """Tests constructor"""
        r = RegisterDataConfig(
            asset_name="some_asset_name",
            mount="asset_mount",
            bucket="asset_bucket",
            prefix="asset_prefix",
            public=True,
            keep_on_external_storage=True,
            tags=["a", "b"],
            custom_metadata={"key1": "value1", "key2": "value2"},
            viewable_to_everyone=False,
        )
        self.assertEqual("some_asset_name", r.asset_name)
        self.assertEqual("asset_mount", r.mount)
        self.assertEqual("asset_bucket", r.bucket)
        self.assertEqual("asset_prefix", r.prefix)
        self.assertTrue(r.public)
        self.assertTrue(r.keep_on_external_storage)
        self.assertEqual(["a", "b"], r.tags)
        self.assertEqual(
            {"key1": "value1", "key2": "value2"}, r.custom_metadata
        )
        self.assertFalse(r.viewable_to_everyone)

    def test_success_default_fields(self):
        """Tests constructor with defaults"""
        r = RegisterDataConfig(
            asset_name="some_asset_name",
            mount="asset_mount",
            bucket="asset_bucket",
            prefix="asset_prefix",
        )
        self.assertEqual("some_asset_name", r.asset_name)
        self.assertEqual("asset_mount", r.mount)
        self.assertEqual("asset_bucket", r.bucket)
        self.assertEqual("asset_prefix", r.prefix)
        self.assertFalse(r.public)
        self.assertTrue(r.keep_on_external_storage)
        self.assertEqual([], r.tags)
        self.assertEqual(None, r.custom_metadata)
        self.assertFalse(r.viewable_to_everyone)

    def test_validator(self):
        """Tests validator"""
        r = RegisterDataConfig(
            asset_name="some_asset_name",
            mount="asset_mount",
            bucket="asset_bucket",
            prefix="asset_prefix",
            tags=None,
            custom_metadata=None,
        )
        self.assertEqual("some_asset_name", r.asset_name)
        self.assertEqual("asset_mount", r.mount)
        self.assertEqual("asset_bucket", r.bucket)
        self.assertEqual("asset_prefix", r.prefix)
        self.assertFalse(r.public)
        self.assertTrue(r.keep_on_external_storage)
        self.assertEqual([], r.tags)
        self.assertEqual({}, r.custom_metadata)
        self.assertFalse(r.viewable_to_everyone)


class TestRunCapsuleConfig(unittest.TestCase):
    """Tests for RunCapsuleConfig class"""

    def test_success_all_fields(self):
        """Tests constructor"""
        r = RunCapsuleConfig(
            capsule_id="123-abc",
            pipeline_id=None,
            data_assets=[
                ComputationDataAsset(id="999888", mount="some_mount"),
                {"id": "12345", "mount": "some_mount_2"},
            ],
            run_parameters=["a", "b"],
            pause_interval=400,
            capsule_version=3,
            timeout_seconds=10000,
        )
        self.assertEqual("123-abc", r.capsule_id)
        self.assertIsNone(r.pipeline_id)
        self.assertEqual(
            [
                ComputationDataAsset(id="999888", mount="some_mount"),
                ComputationDataAsset(id="12345", mount="some_mount_2"),
            ],
            r.data_assets,
        )
        self.assertEqual(["a", "b"], r.run_parameters)
        self.assertEqual(400, r.pause_interval)
        self.assertEqual(3, r.capsule_version)
        self.assertEqual(10000, r.timeout_seconds)

    def test_success_defaults(self):
        """Tests constructor with defaults"""
        r = RunCapsuleConfig(
            capsule_id="123-abc",
        )
        self.assertEqual("123-abc", r.capsule_id)
        self.assertIsNone(r.pipeline_id)
        self.assertEqual(None, r.data_assets)
        self.assertEqual(None, r.run_parameters)
        self.assertEqual(300, r.pause_interval)
        self.assertEqual(None, r.capsule_version)
        self.assertEqual(None, r.timeout_seconds)

    def test_check_data_assets(self):
        """Tests check_data_assets"""
        r = RunCapsuleConfig(capsule_id="123-abc", data_assets=None)
        self.assertEqual([], r.data_assets)

    def test_pipeline_id_validator(self):
        """Tests pipeline_id validator"""
        with self.assertRaises(ValueError) as e1:
            RunCapsuleConfig()

        with self.assertRaises(ValueError) as e2:
            RunCapsuleConfig(capsule_id="123", pipeline_id="456")

        expected_exception1 = (
            "1 validation error for RunCapsuleConfig\n"
            "pipeline_id\n"
            "  Value error, Either capsule_id or pipeline must be set."
            " [type=value_error, input_value=None, input_type=NoneType]\n"
            "    For further information visit"
            f" https://errors.pydantic.dev/{PYD_VERSION}/v/value_error"
        )

        expected_exception2 = (
            "1 validation error for RunCapsuleConfig\n"
            "pipeline_id\n"
            "  Value error, Only one of capsule_id or pipeline_id can be set."
            " [type=value_error, input_value='456', input_type=str]\n"
            "    For further information visit"
            f" https://errors.pydantic.dev/{PYD_VERSION}/v/value_error"
        )
        self.assertEqual(expected_exception1, repr(e1.exception))
        self.assertEqual(expected_exception2, repr(e2.exception))


class TestCaptureResultConfig(unittest.TestCase):
    """Tests for CaptureResultConfig class"""

    def test_success_all_fields(self):
        """Tests constructor"""
        c = CaptureResultConfig(
            process_name="some_process",
            mount="some_mount",
            asset_name="some_asset",
            tags=["a", "b"],
            custom_metadata={"key1": "value1", "key2": "value2"},
            viewable_to_everyone=True,
        )
        self.assertEqual("some_process", c.process_name)
        self.assertEqual("some_mount", c.mount)
        self.assertEqual("some_asset", c.asset_name)
        self.assertEqual(["a", "b"], c.tags)
        self.assertEqual(
            {"key1": "value1", "key2": "value2"}, c.custom_metadata
        )
        self.assertTrue(c.viewable_to_everyone)

    def test_success_defaults(self):
        """Tests constructor with defaults"""
        c = CaptureResultConfig(process_name="some_process")
        self.assertEqual("some_process", c.process_name)
        self.assertEqual(None, c.mount)
        self.assertEqual(None, c.asset_name)
        self.assertEqual([], c.tags)
        self.assertEqual({}, c.custom_metadata)
        self.assertFalse(c.viewable_to_everyone)

    def test_check_tags(self):
        """Tests check_tags validator"""
        c = CaptureResultConfig(process_name="some_process", tags=None)
        self.assertEqual([], c.tags)

    def test_check_custom_metadata(self):
        """Tests check_custom_metadata validator"""
        c = CaptureResultConfig(
            process_name="some_process", custom_metadata=None
        )
        self.assertEqual({}, c.custom_metadata)

    def test_asset_name_validator(self):
        """Tests asset_name validator"""
        with self.assertRaises(ValueError) as e:
            CaptureResultConfig()

        expected_exception = (
            "1 validation error for CaptureResultConfig\n"
            "asset_name\n"
            "  Value error, Either asset_name or process_name must be provided"
            " [type=value_error, input_value=None, input_type=NoneType]\n"
            "    For further information visit"
            f" https://errors.pydantic.dev/{PYD_VERSION}/v/value_error"
        )
        self.assertEqual(expected_exception, repr(e.exception))


class TestCodeOceanJobConfig(unittest.TestCase):
    """Tests for CodeOceanJobConfig class"""

    def test_success_all_fields(self):
        """Tests constructor"""
        register_config = RegisterDataConfig(
            asset_name="some_asset_name",
            mount="asset_mount",
            bucket="asset_bucket",
            prefix="asset_prefix",
        )
        run_capsule_config = RunCapsuleConfig(
            capsule_id="123-abc",
        )
        capture_result_config = CaptureResultConfig(
            process_name="some_process"
        )
        c = CodeOceanJobConfig(
            register_config=register_config,
            run_capsule_config=run_capsule_config,
            capture_result_config=capture_result_config,
        )

        self.assertEqual(register_config, c.register_config)
        self.assertEqual(run_capsule_config, c.run_capsule_config)
        self.assertEqual(capture_result_config, c.capture_result_config)

    def test_success_defaults(self):
        """Tests constructor with defaults"""
        run_capsule_config = RunCapsuleConfig(
            capsule_id="123-abc",
        )
        c = CodeOceanJobConfig(run_capsule_config=run_capsule_config)
        self.assertEqual(run_capsule_config, c.run_capsule_config)
        self.assertIsNone(c.capture_result_config)
        self.assertIsNone(c.register_config)


if __name__ == "__main__":
    unittest.main()
