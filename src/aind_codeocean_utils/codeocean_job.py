"""
Utility for coordinating registration, processing,
and capture of results in Code Ocean
"""

import logging
import time
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple

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
from aind_data_schema.core.data_description import DataLevel
from aind_data_schema_models.data_name_patterns import datetime_to_name_string
from pydantic import BaseModel, Field

from aind_codeocean_utils.api_handler import APIHandler

logger = logging.getLogger(__name__)


class CustomMetadataKeys(str, Enum):
    """
    Keys used for custom metadata in Code OCean
    """

    DATA_LEVEL = "data level"


def construct_asset_tags_and_metadata(
    asset_name: str, tags: list = None, custom_metadata: dict = None
) -> tuple:
    """Construct metadata for new data assets"""
    tags = set(tags) if tags is not None else set()
    custom_metadata = custom_metadata or dict()
    custom_metadata = custom_metadata.copy()

    tokens = asset_name.split("_")
    if len(tokens) >= 2:
        platform, subject_id = tokens[0], tokens[1]

        tags.update((platform, subject_id))

        custom_metadata.update(
            {
                "experiment type": platform,
                "subject id": subject_id,
            }
        )

    return tags, custom_metadata


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

    tags = sorted(list(tags))

    custom_metadata = custom_metadata or {}
    custom_metadata.update(
        {CustomMetadataKeys.DATA_LEVEL.value: data_level.value}
    )

    return tags, custom_metadata


class ProcessConfig(BaseModel):
    """
    Settings for processing data
    """

    request: RunCapsuleRequest = Field(
        description="Request to run a capsule or pipeline."
    )
    input_data_asset_mount: Optional[str] = Field(
        default=None,
        description=(
            "Mount point for the input data asset. "
            "This is only used to specify the mount of a newly registered "
            "data asset."
        ),
    )
    poll_interval_seconds: Optional[int] = Field(
        default=300,
        description=(
            "Time in seconds to wait between polling for the completion of "
            "the computation."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Time in seconds to wait for the computation to complete. "
            "If None, the computation will be polled indefinitely."
        ),
    )


class CaptureConfig(BaseModel):
    """
    Settings for capturing results
    """

    request: Optional[CreateDataAssetRequest] = Field(
        default=None,
        description=(
            "Request to create a data asset based on a processed result."
        ),
    )

    process_name: Optional[str] = Field(
        default="processed", description="Name of the process."
    )
    input_data_asset_name: Optional[str] = Field(
        default=None, description="Name of the input data asset."
    )


class CodeOceanJobConfig(BaseModel):
    """
    Class for coordinating registration, processing, and capture of results in
    Code Ocean
    """

    register_config: Optional[CreateDataAssetRequest] = None
    process_config: Optional[ProcessConfig] = None
    capture_config: Optional[CaptureConfig] = None
    assets_viewable_to_everyone: bool = Field(
        default=True,
        description=(
            "Whether the assets should be viewable to everyone. "
            "If True, the assets will be viewable to everyone. "
            "If False, the assets will be viewable only to the user who "
            "registered them."
        ),
    )
    add_subject_and_platform_metadata: bool = Field(
        default=True,
        description=(
            "Whether to add metadata about the subject and platform to the "
            "data assets."
        ),
    )
    add_data_level_metadata: bool = Field(
        default=True,
        description=(
            "Whether to add metadata about the data level to the data assets."
        ),
    )


class CodeOceanJob:
    """
    Class for coordinating registration, processing, and capture of results in
    Code Ocean
    """

    def __init__(
        self, co_client: CodeOceanClient, job_config: CodeOceanJobConfig
    ):
        """
        The CodeOceanJob constructor
        """
        job_config = job_config.model_copy(deep=True)
        self.api_handler = APIHandler(co_client=co_client)
        self.register_config = job_config.register_config
        self.process_config = job_config.process_config
        self.capture_config = job_config.capture_config
        self.assets_viewable_to_everyone = (
            job_config.assets_viewable_to_everyone
        )
        self.add_data_level_metadata = job_config.add_data_level_metadata
        self.add_subject_and_platform_metadata = (
            job_config.add_subject_and_platform_metadata
        )

    def run_job(self):
        """Run the job."""

        register_data_response = None
        process_response = None
        capture_response = None

        if self.capture_config:
            assert (
                self.process_config is not None
            ), "process_config must be provided to capture results"

        if self.register_config:
            register_data_response = self.register_data(
                request=self.register_config
            )

        if self.process_config:
            process_response = self.process_data(
                register_data_response=register_data_response
            )

        if self.capture_config:
            capture_response = self.capture_result(
                process_response=process_response
            )

        return register_data_response, process_response, capture_response

    def register_data(
        self, request: CreateDataAssetRequest
    ) -> requests.Response:
        """Register the data asset, also handling metadata tagging."""
        tags = request.tags or []
        custom_metadata = request.custom_metadata or {}
        if self.add_subject_and_platform_metadata:
            tags, custom_metadata = construct_asset_tags_and_metadata(
                request.name, tags, custom_metadata
            )
        if self.add_data_level_metadata:
            tags, custom_metadata = add_data_level_metadata(
                DataLevel.RAW,
                tags,
                custom_metadata,
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
        else:
            assert isinstance(
                self.process_config.request.data_assets, list
            ), "data_assets must be a list"
            if isinstance(self.process_config.request.data_assets[0], dict):
                self.process_config.request.data_assets = [
                    ComputationDataAsset(**asset)
                    for asset in self.process_config.request.data_assets
                ]

        if register_data_response:
            input_data_asset_id = register_data_response.json()["id"]

            if self.process_config.input_data_asset_mount:
                input_data_asset_mount = (
                    self.process_config.input_data_asset_mount
                )
            else:
                input_data_asset_mount = self.register_config.mount

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
                    (self.process_config.timeout_seconds is not None)
                    and (
                        self.process_config.poll_interval_seconds * num_checks
                        >= self.process_config.timeout_seconds
                    )
                ):
                    executing = False
        return run_capsule_response

    def capture_result(  # noqa: C901
        self, process_response: requests.Response
    ) -> requests.Response:
        """Capture the result of the processing that just finished."""

        computation_id = process_response.json()["id"]

        create_data_asset_request = self.capture_config.request
        if create_data_asset_request is None:
            create_data_asset_request = CreateDataAssetRequest(
                name=None,
                mount=None,
                tags=[],
                custom_metadata={},
            )

        input_data_asset_name = self.capture_config.input_data_asset_name

        if create_data_asset_request.name is None:
            if self.register_config is not None:
                asset_name = build_processed_data_asset_name(
                    self.register_config.name,
                    self.capture_config.process_name,
                )
                # add input tags and custom metadata to result asset
                create_data_asset_request.tags.extend(
                    self.register_config.tags
                )
                create_data_asset_request.custom_metadata.update(
                    self.register_config.custom_metadata
                )
            elif (
                self.process_config is not None
                and self.process_config.request.data_assets is not None
            ):
                data_asset_ids = self.process_config.request.data_assets
                # for single input data asset, use input data asset name
                assert isinstance(
                    data_asset_ids, list
                ), "data_assets must be a list"
                # make sure data_assets is a list of ComputationDataAsset
                if isinstance(data_asset_ids[0], dict):
                    data_asset_ids = [
                        ComputationDataAsset(**asset)
                        for asset in data_asset_ids
                    ]
                if len(data_asset_ids) > 1 and input_data_asset_name is None:
                    raise AssertionError(
                        "Data asset name not provided and "
                        "multiple data assets were provided in "
                        "the process configuration"
                    )
                # for multiple input data assets,
                # propagate all tags and custom metadata
                existing_tags = []
                existing_custom_metadata = {}
                for data_asset_id in data_asset_ids:
                    response = self.api_handler.co_client.get_data_asset(
                        data_asset_id.id
                    )
                    response_json = response.json()
                    existing_tags.extend(response_json.get("tags", []))
                    existing_custom_metadata.update(
                        response_json.get("custom_metadata", {})
                    )
                if len(data_asset_ids) == 1:
                    response_json = response.json()
                    input_data_asset_name = (
                        input_data_asset_name or response_json["name"]
                    )
                asset_name = build_processed_data_asset_name(
                    input_data_asset_name,
                    self.capture_config.process_name,
                )
                # add input tags and custom metadata to result asset
                create_data_asset_request.tags.extend(existing_tags)
                create_data_asset_request.custom_metadata.update(
                    existing_custom_metadata
                )
            else:
                assert input_data_asset_name is not None, (
                    "Data asset name not provided"
                )

            create_data_asset_request.name = asset_name

        if create_data_asset_request.mount is None:
            create_data_asset_request.mount = create_data_asset_request.name

        create_data_asset_request.source = Source(
            computation=Sources.Computation(id=computation_id)
        )

        if self.add_data_level_metadata:
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
