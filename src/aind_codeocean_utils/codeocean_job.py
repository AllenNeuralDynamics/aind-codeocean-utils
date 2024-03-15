""" Utility for coordinating registration, processing, and capture of results in Code Ocean """

import logging
import time
from datetime import datetime
from typing import List, Optional, Tuple, Union

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
    custom_metadata.update({"data level": data_level.value})

    return tags, custom_metadata


class CaptureResultConfig(BaseModel):
    """
    Settings for capturing results
    """

    process_name: Optional[str] = Field(
        default="processed", description="Name of the process."
    )
    input_data_asset_name: Optional[str] = Field(
        default=None, description="Name of the input data asset."
    )


class CodeOceanJob:
    def __init__(
        self,
        co_client: CodeOceanClient,
        register_data_config: Optional[CreateDataAssetRequest] = None,
        process_config: Optional[RunCapsuleRequest] = None,
        capture_result_config: Optional[
            Union[CaptureResultConfig, CreateDataAssetRequest]
        ] = None,
        assets_viewable_to_everyone: bool = True,
        process_poll_interval_seconds: int = 300,
        process_timeout_seconds: int = None,
        add_data_level_tags: bool = True,
    ):
        self.api_handler = APIHandler(co_client=co_client)
        self.register_data_config = register_data_config
        self.process_config = process_config
        self.capture_result_config = capture_result_config
        self.assets_viewable_to_everyone = assets_viewable_to_everyone
        self.process_poll_interval_seconds = process_poll_interval_seconds
        self.process_timeout_seconds = process_timeout_seconds
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
    """Process the data, handling the case where the data was just registered upstream."""

        if self.process_config.data_assets is None:
            self.process_config.data_assets = []

        if register_data_response:
            input_data_asset_id = register_data_response.json()["id"]
            input_data_asset_mount = self.register_data_config.mount
            self.process_config.data_assets.append(
                ComputationDataAsset(
                    id=input_data_asset_id, mount=input_data_asset_mount
                )
            )

        self.api_handler.check_data_assets(self.process_config.data_assets)

        run_capsule_response = self.api_handler.co_client.run_capsule(
            self.process_config
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
        if self.process_poll_interval_seconds:
            executing = True
            num_checks = 0
            while executing:
                num_checks += 1
                time.sleep(self.process_poll_interval_seconds)
                computation_response = (
                    self.api_handler.co_client.get_computation(computation_id)
                )
                curr_computation_state = computation_response.json()

                if (curr_computation_state["state"] == "completed") or (
                    (run_capsule_config.timeout_seconds is not None)
                    and (
                        run_capsule_config.process_poll_interval_seconds
                        * num_checks
                        >= run_capsule_config.timeout_seconds
                    )
                ):
                    executing = False
        return run_capsule_response

    def capture_result(
        self, process_response: requests.Response
    ) -> requests.Response:
    """Capture the result of the processing that just finished."""

        computation_id = process_response.json()["id"]

        if isinstance(self.capture_result_config, CreateDataAssetRequest):
            create_data_asset_request = capture_result_config
        elif isinstance(self.capture_result_config, CaptureResultConfig):
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
