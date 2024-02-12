"""Models for config files."""
from typing import Dict, List, Optional

from aind_codeocean_api.models.computations_requests import (
    ComputationDataAsset,
)
from pydantic import BaseModel, Field, ValidationInfo, field_validator


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
    tags: List[str] = Field(
        default=[], description="The tags to use to describe the data asset."
    )
    custom_metadata: Optional[Dict] = Field(
        default=None,
        description="What key:value metadata tags to apply to the asset.",
    )
    viewable_to_everyone: bool = Field(
        default=False,
        description="Whether to share the captured results with everyone.",
    )

    @field_validator("tags", mode="before")
    def check_tags(cls, v: Optional[List[str]]) -> List[str]:
        """Allows user to input None and converts them to empty collection"""
        if v is not None:
            return v
        else:
            return []

    @field_validator("custom_metadata", mode="before")
    def check_custom_metadata(cls, v: Optional[Dict]) -> Dict:
        """Allows user to input None and converts them to empty collection"""
        if v is not None:
            return v
        else:
            return dict()


class RunCapsuleConfig(BaseModel):
    """
    Settings for running a capsule
    """

    capsule_id: Optional[str] = Field(
        default=None, description="ID of the capsule to run."
    )
    pipeline_id: Optional[str] = Field(
        default=None,
        description="ID of the pipeline to run.",
        validate_default=True,
    )
    data_assets: Optional[List[ComputationDataAsset]] = Field(
        default=None,
        description="List of data assets for the capsule to run against.",
    )
    input_data_mount: Optional[str] = Field(
        default=None,
        description=(
            "The mount point for the newly registered input data asset, "
            "if different than the asset mount name."
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

    @field_validator("pipeline_id", mode="after")
    def validate_pipeline_id(
        cls, v: Optional[str], info: ValidationInfo
    ) -> Optional[str]:
        """Check that only one of capsule_id or pipeline_id is set."""
        if info.data.get("capsule_id") is None and v is None:
            raise ValueError("Either capsule_id or pipeline must be set.")
        elif info.data.get("capsule_id") is not None and v is not None:
            raise ValueError(
                "Only one of capsule_id or pipeline_id can be set."
            )
        return v

    @field_validator("data_assets", mode="before")
    def check_data_assets(
        cls, v: Optional[list]
    ) -> Optional[List[ComputationDataAsset]]:
        """
        Coerces dictionaries into ComputationDataAsset type or emtpy list
        """
        if v is None:
            return []
        else:
            updated_list = []
            for item in v:
                if isinstance(item, ComputationDataAsset):
                    updated_list.append(item)
                elif isinstance(item, dict):
                    updated_list.append(ComputationDataAsset(**item))
            return updated_list


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
        default=None,
        description="The name to give the data asset.",
        validate_default=True,
    )
    tags: List[str] = Field(
        default=[], description="The tags to use to describe the data asset."
    )
    custom_metadata: Dict = Field(
        default=dict(),
        description="What key:value metadata tags to apply to the asset.",
    )
    viewable_to_everyone: bool = Field(
        default=False,
        description="Whether to share the captured results with everyone.",
    )

    @field_validator("tags", mode="before")
    def check_tags(cls, v: Optional[List[str]]) -> List[str]:
        """Allows user to input None and converts them to empty collection"""
        if v is not None:
            return v
        else:
            return []

    @field_validator("custom_metadata", mode="before")
    def check_custom_metadata(cls, v: Optional[Dict]) -> Dict:
        """Allows user to input None and converts them to empty collection"""
        if v is not None:
            return v
        else:
            return dict()

    @field_validator("asset_name", mode="after")
    def validate_asset_name(
        cls, v: Optional[str], info: ValidationInfo
    ) -> Optional[str]:
        """Check that asset_name is not None if process_name is None"""
        if info.data.get("process_name") is None and v is None:
            raise ValueError(
                "Either asset_name or process_name must be provided"
            )
        return v


class CodeOceanJobConfig(BaseModel):
    """
    Settings for CodeOceanJob
    """

    register_config: Optional[RegisterDataConfig] = Field(
        default=None, description="Settings for registering data"
    )
    run_capsule_config: RunCapsuleConfig = Field(
        ..., description="Settings for running a capsule"
    )
    capture_result_config: Optional[CaptureResultConfig] = Field(
        default=None, description="Settings for capturing results"
    )
