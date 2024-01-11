"""Models for config files."""
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class RegisterDataConfig(BaseModel):
    """
    Settings for registering data
    """

    asset_name: str = Field(
        ..., description="The name to give the data asset."
    )
    mount: str = Field(..., description="The mount folder name.")
    bucket: str = Field(
        ..., description="The s3 bucket the data asset is located."
    )
    prefix: str = Field(
        ..., description="The s3 prefix where the data asset is located."
    )
    public: bool = Field(
        default=False, description="Whether the data asset is public or not."
    )
    keep_on_external_storage: bool = Field(
        default=True,
        description="Whether to keep the data asset on external storage.",
    )
    tags: Optional[List[str]] = Field(
        default=None, description="The tags to use to describe the data asset."
    )
    custom_metadata: Optional[Dict] = Field(
        default=None,
        description="What key:value metadata tags to apply to the asset.",
    )
    viewable_to_everyone: bool = Field(
        default=False,
        description="Whether to share the captured results with everyone.",
    )

    @model_validator(mode="after")
    def check_tags_and_custom_metadata(self) -> "RegisterDataConfig":
        """Check that tags and custom_metadata are lists and dicts"""
        if self.tags is None:
            self.tags = []
        if self.custom_metadata is None:
            self.custom_metadata = {}
        return self


class RunCapsuleConfig(BaseModel):
    """
    Settings for running a capsule
    """

    capsule_id: Optional[str] = Field(
        default=None, description="ID of the capsule to run."
    )
    pipeline_id: Optional[str] = Field(
        default=None, description="ID of the pipeline to run."
    )
    data_assets: Optional[List[Dict]] = Field(
        default=None,
        description=(
            "List of data assets for the capsule to run against. "
            "The dict should have the keys id and mount."
        ),
    )
    run_parameters: Optional[List] = Field(
        default=None, description="The parameters to pass to the capsule."
    )
    pause_interval: Optional[int] = Field(
        default=300,
        description="How often to check if the capsule run is finished.",
    )
    capsule_version: Optional[int] = Field(
        default=None,
        description="Run a specific version of the capsule to be run.",
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "If pause_interval is set, the max wait time to check if the "
            "capsule is finished."
        ),
    )

    @model_validator(mode="after")
    def check_data_assets(self) -> "RunCapsuleConfig":
        """
        Check that data_assets is a list of dicts with keys 'id' and 'mount'.
        """
        if self.data_assets is not None:
            assert isinstance(
                self.data_assets, (list, tuple)
            ), "data_assets must be a list or tuple"
            assert all(
                [
                    "id" in data_asset and "mount" in data_asset
                    for data_asset in self.data_assets
                ]
            ), "data_assets must be a list of dicts with keys 'id' and 'mount'"
        else:
            self.data_assets = []
        return self


class CaptureResultConfig(BaseModel):
    """
    Settings for capturing results
    """

    process_name: Optional[str] = Field(
        default=None, description="Name of the process."
    )
    mount: Optional[str] = Field(
        default=None, description="The mount folder name."
    )
    asset_name: Optional[str] = Field(
        default=None, description="The name to give the data asset."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="The tags to use to describe the data asset."
    )
    custom_metadata: Optional[Dict] = Field(
        default=None,
        description="What key:value metadata tags to apply to the asset.",
    )
    viewable_to_everyone: bool = Field(
        default=False,
        description="Whether to share the captured results with everyone.",
    )

    @model_validator(mode="after")
    def check_asset_and_metadata(self) -> "CaptureResultConfig":
        """Check that asset_name and custom_metadata are lists and dicts"""
        if self.asset_name is None:
            assert (
                self.process_name is not None
            ), "Either asset_name or process_name must be provided"
        if self.tags is None:
            self.tags = []
        if self.custom_metadata is None:
            self.custom_metadata = {}
        return self


class CodeOceanJobConfig(BaseModel):
    """
    Settings for CodeOceanJob
    """

    register_config: Optional[RegisterDataConfig] = Field(
        default=None, description="Settings for registering data"
    )
    run_capsule_config: RunCapsuleConfig = Field(
        description="Settings for running a capsule"
    )
    capture_result_config: Optional[CaptureResultConfig] = Field(
        default=None, description="Settings for capturing results"
    )
