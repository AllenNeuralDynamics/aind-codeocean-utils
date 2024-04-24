"""
Utility for coordinating registration, processing,
and capture of results in Code Ocean
"""

import logging
import time
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union
from enum import Enum

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
from aind_data_schema.core.data_description import (
    DataLevel,
    datetime_to_name_string,
)
from pydantic import BaseModel, Field

from aind_codeocean_utils.api_handler import APIHandler

logger = logging.getLogger(__name__)


class CustomMetadataKeys(str, Enum):
    """
    Keys used for custom metadata in Code OCean
    """

    DATA_LEVEL = "data level"


def build_processed_data_asset_name(input_data_asset_name, process_name):
    """Build a name for a processed data asset."""

    capture_time = datetime_to_name_string(datetime.now())

    return f"{input_data_asset_name}_{process_name}_{capture_time}"


def add_data_level_metadata(
    data_level: DataLevel,
    tags: List[str] = None,
    custom_metadata: dict = None,
) -> Tuple[List[str], dict]:
    """Add data level metadata to tags and custom metadata."""

    tags = set(tags or [])
    tags.add(data_level.value)

    if data_level == DataLevel.DERIVED:
        tags.discard(DataLevel.RAW.value)

    tags = list(tags)

    custom_metadata = custom_metadata or {}
    custom_metadata.update(
        {CustomMetadataKeys.DATA_LEVEL.value: data_level.value}
    )

    return tags, custom_metadata


class ProcessConfig(BaseModel):
    """
    Settings for processing data
    """

    request: RunCapsuleRequest
    input_data_asset_mount: Optional[str] = None
    poll_interval_seconds: Optional[int] = (300,)
    timeout_seconds: Optional[int] = None


class CaptureResultConfig(BaseModel):
    """
    Settings for capturing results
    """

    request: Optional[CreateDataAssetRequest] = Field(
        default=None,
        description="Request to create a data asset based on a processed result.",
    )

    process_name: Optional[str] = Field(
        default="processed", description="Name of the process."
    )
    input_data_asset_name: Optional[str] = Field(
        default=None, description="Name of the input data asset."
    )


class CodeOceanJob:
    """
    Class for coordinating registration, processing, and capture of results in
    Code Ocean
    """
    def __init__(
        self,
        co_client: CodeOceanClient,
        register_data_config: Optional[CreateDataAssetRequest] = None,
        process_config: Optional[ProcessConfig] = None,
        capture_result_config: Optional[CaptureResultConfig] = None,
        assets_viewable_to_everyone: bool = True,
        add_data_level_tags: bool = True,
    ):
        """
        Class constructor
        Parameters
        ----------
        co_client : CodeOceanClient
            Code Ocean client
        register_data_config : Optional[CreateDataAssetRequest]
            Configuration for registering data
        process_config : Optional[RunCapsuleRequest]
            Configuration for processing data
        capture_result_config : CaptureResultConfi or CreateDataAssetRequest
            Configuration for capturing results
        assets_viewable_to_everyone : bool
            Whether newly registered and processed assets should be
            viewable to everyone
        process_poll_interval_seconds : int
            Interval in seconds for polling the process
        process_timeout_seconds : int
            Timeout in seconds for the process
        add_data_level_tags : bool
            Whether to add data level tags to assets
        """
        self.api_handler = APIHandler(co_client=co_client)
        self.register_data_config = register_data_config
        self.process_config = process_config
        self.capture_result_config = capture_result_config
        self.assets_viewable_to_everyone = assets_viewable_to_everyone
        self.add_data_level_tags = add_data_level_tags

    def run_job(self):
        """Run the job."""

        register_data_response = None
        process_response = None
        capture_response = None

        if self.capture_result_config:
            assert (
                self.process_config is not None
            ), "process_config must be provided to capture results"

        if self.register_data_config:
            register_data_response = self.register_data(
                request=self.register_data_config
            )

        if self.process_config:
            process_response = self.process_data(
                register_data_response=register_data_response
            )

        if self.capture_result_config:
            capture_response = self.capture_result(
                process_response=process_response
            )

        return register_data_response, process_response, capture_response

    def register_data(
        self, request: CreateDataAssetRequest
    ) -> requests.Response:
        """Register the data asset, also handling metadata tagging."""
        if self.add_data_level_tags:
            tags, custom_metadata = add_data_level_metadata(
                DataLevel.RAW,
                request.tags,
                request.custom_metadata,
            )
            request.tags = tags
            request.custom_metadata = custom_metadata

        # TODO handle non-aws sources
        if request.source.aws is not None:
            assert (
                request.source.aws.keep_on_external_storage is True
            ), "Data assets must be kept on external storage."

        response = self.api_handler.create_data_asset_and_update_permissions(
            request=request,
            assets_viewable_to_everyone=self.assets_viewable_to_everyone,
        )

        return response

    def process_data(
        self, register_data_response: requests.Response = None
    ) -> requests.Response:
        """Process the data, handling the case where the data was just
        registered upstream."""

        if self.process_config.request.data_assets is None:
            self.process_config.request.data_assets = []

        if register_data_response:
            input_data_asset_id = register_data_response.json()["id"]

            if self.process_config.input_data_asset_mount:
                input_data_asset_mount = (
                    self.process_config.input_data_asset_mount
                )
            else:
                input_data_asset_mount = self.register_data_config.mount

            self.process_config.request.data_assets.append(
                ComputationDataAsset(
                    id=input_data_asset_id, mount=input_data_asset_mount
                )
            )

        self.api_handler.check_data_assets(
            self.process_config.request.data_assets
        )

        run_capsule_response = self.api_handler.co_client.run_capsule(
            self.process_config.request
        )
        run_capsule_response_json = run_capsule_response.json()

        if run_capsule_response_json.get("id") is None:
            raise KeyError(
                f"Something went wrong running the capsule or pipeline. "
                f"Response Status Code: {run_capsule_response.status_code}. "
                f"Response Message: {run_capsule_response_json}"
            )

        computation_id = run_capsule_response_json["id"]

        # TODO: We may need to clean up the loop termination logic
        if self.process_config.poll_interval_seconds:
            executing = True
            num_checks = 0
            while executing:
                num_checks += 1
                time.sleep(self.process_config.poll_interval_seconds)
                computation_response = (
                    self.api_handler.co_client.get_computation(computation_id)
                )
                curr_computation_state = computation_response.json()

                if (curr_computation_state["state"] == "completed") or (
                    (self.process_timeout_seconds is not None)
                    and (
                        self.process_poll_interval_seconds
                        * num_checks
                        >= self.process_timeout_seconds
                    )
                ):
                    executing = False
        return run_capsule_response

    def capture_result(
        self, process_response: requests.Response
    ) -> requests.Response:
        """Capture the result of the processing that just finished."""

        computation_id = process_response.json()["id"]

        create_data_asset_request = self.capture_result_config.request
        if create_data_asset_request is None:
            create_data_asset_request = CreateDataAssetRequest(
                name=None,
                mount=None,
                tags=[],
                custom_metadata={},
            )

            asset_name = None
            if self.capture_result_config.input_data_asset_name is not None:
                asset_name = self.capture_result_config.input_data_asset_name
            elif self.register_data_config is not None:
                asset_name = self.register_data_config.name
            else:
                raise ValueError("could not determine captured asset name")

            asset_name = build_processed_data_asset_name(
                asset_name,
                self.capture_result_config.process_name,
            )
            create_data_asset_request.name = asset_name
            create_data_asset_request.mount = asset_name

        create_data_asset_request.source = Source(
            computation=Sources.Computation(id=computation_id)
        )

        if self.add_data_level_tags:
            tags, custom_metadata = add_data_level_metadata(
                DataLevel.DERIVED,
                create_data_asset_request.tags,
                create_data_asset_request.custom_metadata,
            )
            create_data_asset_request.tags = tags
            create_data_asset_request.custom_metadata = custom_metadata

        capture_result_response = (
            self.api_handler.create_data_asset_and_update_permissions(
                request=create_data_asset_request,
                assets_viewable_to_everyone=self.assets_viewable_to_everyone,
            )
        )

        return capture_result_response
