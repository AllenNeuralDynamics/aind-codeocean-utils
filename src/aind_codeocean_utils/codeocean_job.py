"""Module with generic Code Ocean job"""
import logging
import time
from copy import deepcopy
from datetime import datetime
from typing import List, Optional

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
from aind_codeocean_utils.models.config import (
    CaptureResultConfig,
    CodeOceanJobConfig,
    RegisterDataConfig,
    RunCapsuleConfig,
)

logger = logging.getLogger(__name__)


class CodeOceanJob:
    """
    This class contains convenient methods to register data assets,
    run capsules, and capture results.
    """

    def __init__(
        self,
        co_client: CodeOceanClient,
        job_config: CodeOceanJobConfig,
    ):
        """
        CapsuleJob class constructor.

        Parameters
        ----------
        co_client : CodeOceanClient
            A client that can be used to interface with the Code Ocean API.
        job_config : CodeOceanJobConfig
            Configuration parameters for the job.

            * register_config : optional
                Configuration parameters for registering data assets,
                including:

                * asset_name : str
                    The name to give the data asset
                * mount : str
                    The mount folder name
                * bucket : str
                    The s3 bucket the data asset is located.
                * prefix : str
                    The s3 prefix where the data asset is located.
                * public : bool
                    Whether the bucket is public or not. Default is False.

            * run_capsule_config : required
                Configuration parameters for running a capsule, including:

                * capsule_id : str or None
                    ID of the capsule or pipeline to run.
                * pipeline_id : str or None
                    ID of the pipeline to run.
                * data_assets : List[Dict]
                    List of data assets for the capsule to run against.
                    Each entry dict should have the 'keys' id and 'mount'.
                * run_parameters : Optional[List]
                    List of parameters to pass to the capsule.
                * pause_interval : Optional[int]
                    How often to check if the capsule run is finished.
                    If None, then the method will return immediately without
                    waiting for the computation to finish.
                * capsule_version : Optional[int]
                    Run a specific version of the capsule to be run
                    Default is None.
                * timeout_seconds : Optional[int]
                    If pause_interval is set, the max wait time to check if
                    the capsule is finished.

            * capture_result_config : optional
                Configuration parameters for capturing results, including:

                * process_name : Optional[str]
                    Name of the process. When it is provided and asset_name is
                    None, the output data asset name will be:
                    {input_data_asset_name}_{process_name}_{capture_time}.
                    Note that if multiple input data assets are provided,
                    then the asset_name will be required.
                * mount : Optional[str]
                    The mount folder name. If None, then the mount folder name
                    will be the same as the asset_name.
                * asset_name : Optional[str]
                    The name to give the data asset. If multiple input data
                    assets are provided,
                    then this must be provided.
                * tags : Optional[List[str]]
                    The tags to add to describe the output data asset in
                    addition to the input data asset tags.
                    In case multiple input data assets are provided,
                    the input data tags are not propagated to the
                    output data asset.
                * custom_metadata : Optional[dict]
                    What key:value metadata tags to add to the output data
                    asset in addition to the input data asset
                    custom_metadata. In case multiple input data assets are
                    provided, the input data custom_metadata is not propagated
                    to the output data asset.
                * viewable_to_everyone : bool
                    Whether to share the captured results with everyone.
        """
        self.co_client = co_client
        self.job_config = job_config

    def _wait_for_data_availability(
        self,
        data_asset_id: str,
        timeout_seconds: int = 300,
        pause_interval=10,
    ) -> requests.Response:
        """
        There is a lag between when a register data request is made and
        when the data is available to be used in a capsule.
        Parameters
        ----------
        data_asset_id : str
            ID of the data asset to check for.
        timeout_seconds : int
            Roughly how long the method should check if the data is available.
        pause_interval : int
            How many seconds between when the backend is queried.

        Returns
        -------
        requests.Response

        """
        num_of_checks = 0
        break_flag = False
        time.sleep(pause_interval)
        response = self.co_client.get_data_asset(data_asset_id)
        if ((pause_interval * num_of_checks) > timeout_seconds) or (
            response.status_code == 200
        ):
            break_flag = True
        while not break_flag:
            time.sleep(pause_interval)
            response = self.co_client.get_data_asset(data_asset_id)
            num_of_checks += 1
            if ((pause_interval * num_of_checks) > timeout_seconds) or (
                response.status_code == 200
            ):
                break_flag = True
        return response

    def check_data_assets(
        self, data_assets: List[ComputationDataAsset]
    ) -> None:
        """
        Check if data assets exist.

        Parameters
        ----------
        data_assets : list
            List of data assets to check for.

        Raises
        ------
        FileNotFoundError
            If a data asset is not found.
        ConnectionError
            If there is an issue retrieving a data asset.
        """
        for data_asset in data_assets:
            assert isinstance(
                data_asset, ComputationDataAsset
            ), "Data assets must be of type ComputationDataAsset"
            data_asset_id = data_asset.id
            response = self.co_client.get_data_asset(data_asset_id)
            if response.status_code == 404:
                raise FileNotFoundError(f"Unable to find: {data_asset_id}")
            elif response.status_code != 200:
                raise ConnectionError(
                    f"There was an issue retrieving: {data_asset_id}"
                )

    def _run_capsule(
        self,
        run_capsule_config: RunCapsuleConfig,
        input_data_assets: List[ComputationDataAsset] = None,
    ) -> requests.Response:
        """
        Run a specified capsule with the given data assets. If the
        pause_interval is set, the method will return until the capsule run is
        finished before returning a response. If pause_interval is set, then
        the timeout_seconds can also optionally be set to set a max wait time.

        Parameters
        ----------
        run_capsule_config : RunCapsuleConfig
            The configuration for the capsule run, including:

            * capsule_id: str
                ID of the Code Ocean capsule to be run
            * pipeline_id : str
                ID of the Code Ocean pipeline to be run
            * data_assets : List[ComputationDataAsset]
                List of data assets for the capsule to run against. The dict
                should have the keys id and mount.
            * run_parameters : Optional[List]
                List of parameters to pass to the capsule.
            pause_interval : Optional[int]
                How often to check if the capsule run is finished.
                If None, then the method will return immediately without
                waiting for the computation to finish.
            capsule_version : Optional[int]
                Run a specific version of the capsule to be run
            timeout_seconds : Optional[int]
                If pause_interval is set, the max wait time to check if the
                capsule is finished.
        input_data_assets : List[ComputationDataAsset]
            List of additional data assets to run the computation with.
            These can be, for example, data assets that were registered.

        Returns
        -------
        requests.Response

        """
        data_assets = []
        if run_capsule_config.data_assets is not None:
            # check if data assets exist
            data_assets += run_capsule_config.data_assets
        if input_data_assets is not None:
            data_assets += input_data_assets
        self.check_data_assets(data_assets)

        run_capsule_request = RunCapsuleRequest(
            capsule_id=run_capsule_config.capsule_id,
            pipeline_id=run_capsule_config.pipeline_id,
            data_assets=data_assets,
            parameters=run_capsule_config.run_parameters,
            version=run_capsule_config.capsule_version,
        )
        # TODO: Handle case of bad response from code ocean
        run_capsule_response = self.co_client.run_capsule(run_capsule_request)
        run_capsule_response_json = run_capsule_response.json()
        computation_id = run_capsule_response_json["id"]

        # TODO: We may need to clean up the loop termination logic
        if run_capsule_config.pause_interval:
            executing = True
            num_checks = 0
            while executing:
                num_checks += 1
                time.sleep(run_capsule_config.pause_interval)
                computation_response = self.co_client.get_computation(
                    computation_id
                )
                curr_computation_state = computation_response.json()

                if (curr_computation_state["state"] == "completed") or (
                    (run_capsule_config.timeout_seconds is not None)
                    and (
                        run_capsule_config.pause_interval * num_checks
                        >= run_capsule_config.timeout_seconds
                    )
                ):
                    executing = False
        return run_capsule_response

    def _register_data_and_update_permissions(
        self, register_data_config: RegisterDataConfig
    ) -> requests.Response:
        """
        Register a data asset. Can also optionally update the permissions on
        the data asset.

        Parameters
        ----------
        register_data_config : RegisterDataConfig
            The configuration for registering the data asset, including:

            * asset_name : str
                The name to give the data asset
            * mount : str
                The mount folder name
            * bucket : str
                The s3 bucket the data asset is located.
            * prefix : str
                The s3 prefix where the data asset is located.
            * public : bool
                Whether the data asset is public or not. Default is False.
            * keep_on_external_storage : bool
                Whether to keep the data asset on external storage.
                Default is True.
            * tags : List[str]
                The tags to use to describe the data asset
            * custom_metadata : Optional[dict]
                What key:value metadata tags to apply to the asset.
            * viewable_to_everyone : bool
                If set to true, then the data asset will be shared with
                everyone. Default is false.

        Notes
        -----
        The credentials for the s3 bucket must be set in the environment.

        Returns
        -------
        requests.Response
        """
        aws_source = Sources.AWS(
            bucket=register_data_config.bucket,
            prefix=register_data_config.prefix,
            keep_on_external_storage=(
                register_data_config.keep_on_external_storage
            ),
            public=register_data_config.public,
        )
        source = Source(aws=aws_source)
        create_data_asset_request = CreateDataAssetRequest(
            name=register_data_config.asset_name,
            tags=register_data_config.tags,
            mount=register_data_config.mount,
            source=source,
            custom_metadata=register_data_config.custom_metadata,
        )
        data_asset_reg_response = self.co_client.create_data_asset(
            create_data_asset_request
        )

        if register_data_config.viewable_to_everyone:
            response_contents = data_asset_reg_response.json()
            data_asset_id = response_contents["id"]
            response_data_available = self._wait_for_data_availability(
                data_asset_id
            )

            if response_data_available.status_code != 200:
                raise FileNotFoundError(f"Unable to find: {data_asset_id}")

            # Make data asset viewable to everyone
            update_data_perm_response = self.co_client.update_permissions(
                data_asset_id=data_asset_id, everyone="viewer"
            )
            logger.info(
                "Permissions response: "
                f"{update_data_perm_response.status_code}"
            )

        return data_asset_reg_response

    def _capture_result(
        self,
        computation_id: str,
        input_data_asset_name: Optional[str],
        capture_result_config: CaptureResultConfig,
        additional_tags: Optional[list] = None,
        additional_custom_metadata: Optional[dict] = None,
    ) -> requests.Response:
        """
        Capture a result as a data asset. Can also share it with everyone.

        Parameters
        ----------
        computation_id : str
            ID of the computation to capture the result from.
        input_data_asset_name : Optional[str]
            Name of the input data asset to use to create the result name.
        capture_result_config : CaptureResultConfig
            The configuration for capturing the result, including:

            * process_name : Optional[str]
                Name of the process.
            * mount : str
                The mount folder name.
            * asset_name : Optional[str]
                The name to give the data asset.
            * tags : Optional[List[str]]
                The tags to use to describe the data asset.
            * custom_metadata : Optional[dict]
                What key:value metadata tags to apply to the asset.
            * viewable_to_everyone : bool
                Whether to share the captured results with everyone.
        additional_tags : Optional[List[str]]
            Additional tags to add to the data asset. If a data level RAW tag
            is provided, it will be converted to DERIVED.
        additional_custom_metadata : Optional[dict]
            Additional custom metadata to add to the data asset. If a data
            level custom metadata is provided, it will be converted to
            DERIVED.

        Returns
        -------
        requests.Response

        """
        if capture_result_config.asset_name is None:
            assert (
                input_data_asset_name is not None
            ), "Either asset_name or input_data_asset_name must be provided"
            capture_time = datetime_to_name_string(datetime.now())
            asset_name = (
                f"{input_data_asset_name}"
                f"_{capture_result_config.process_name}"
                f"_{capture_time}"
            )
        else:
            asset_name = capture_result_config.asset_name

        if capture_result_config.mount is None:
            mount = asset_name
        else:
            mount = capture_result_config.mount

        tags = capture_result_config.tags.copy()
        if additional_tags is not None:
            # skip duplicates and set data level to derived
            tags += [
                DataLevel.DERIVED.value if x == DataLevel.RAW.value else x
                for x in additional_tags
                if x not in tags
            ]
        custom_metadata = deepcopy(capture_result_config.custom_metadata)
        if additional_custom_metadata is not None:
            custom_metadata.update(additional_custom_metadata)

        # TODO: pull this 'data level' string from a controlled vocabulary
        if (
            "data level" in custom_metadata
            and custom_metadata["data level"] == DataLevel.RAW.value
        ):
            custom_metadata["data level"] = DataLevel.DERIVED.value

        computation_source = Sources.Computation(
            id=computation_id,
        )
        source = Source(computation=computation_source)
        create_data_asset_request = CreateDataAssetRequest(
            name=asset_name,
            tags=tags,
            mount=mount,
            source=source,
            custom_metadata=custom_metadata,
        )

        reg_result_response = self.co_client.create_data_asset(
            create_data_asset_request
        )
        registered_results_response_json = reg_result_response.json()

        # TODO: This step intermittently breaks. Adding extra check to help
        #  figure out why.
        if registered_results_response_json.get("id") is None:
            raise KeyError(
                f"Something went wrong registering"
                f" {asset_name}. "
                f"Response Status Code: {reg_result_response.status_code}. "
                f"Response Message: {registered_results_response_json}"
            )

        results_data_asset_id = registered_results_response_json["id"]
        response_res_available = self._wait_for_data_availability(
            data_asset_id=results_data_asset_id
        )

        if response_res_available.status_code != 200:
            raise FileNotFoundError(f"Unable to find: {results_data_asset_id}")

        # Make captured results viewable to everyone
        if capture_result_config.viewable_to_everyone:
            update_res_perm_response = self.co_client.update_permissions(
                data_asset_id=results_data_asset_id, everyone="viewer"
            )
            logger.info(
                f"Updating permissions {update_res_perm_response.status_code}"
            )
        return reg_result_response

    def run_job(self) -> dict:
        """
        Method to process data with a Code Ocean capsule/pipeline.
        """
        responses = dict()

        # 1. register data assets (optional)
        input_data_assets = None
        if self.job_config.register_config is not None:
            logger.info(
                "Registering data asset "
                f"{self.job_config.register_config.prefix}"
            )
            register_data_asset_response = (
                self._register_data_and_update_permissions(
                    self.job_config.register_config
                )
            )
            register_data_asset_response_json = (
                register_data_asset_response.json()
            )
            if self.job_config.run_capsule_config.input_data_mount:
                input_data_mount = (
                    self.job_config.run_capsule_config.input_data_mount
                )
            else:
                input_data_mount = self.job_config.register_config.mount
            input_data_assets = [
                ComputationDataAsset(
                    id=register_data_asset_response_json["id"],
                    mount=input_data_mount,
                )
            ]
            data_asset_tags = self.job_config.register_config.tags
            data_asset_custom_metadata = deepcopy(
                self.job_config.register_config.custom_metadata
            )
            data_asset_name = register_data_asset_response_json["name"]
            responses["register"] = register_data_asset_response
        elif (
            self.job_config.run_capsule_config.data_assets is not None
            and len(self.job_config.run_capsule_config.data_assets) == 1
        ):
            data_asset = self.job_config.run_capsule_config.data_assets[0]
            # TODO: Check status_code and handle other than 200
            data_asset_response = self.co_client.get_data_asset(data_asset.id)
            data_asset_json = data_asset_response.json()
            data_asset_name = data_asset_json["name"]
            data_asset_tags = data_asset_json.get("tags")
            # some of the older data assets don't have a custom_metadata field
            data_asset_custom_metadata = data_asset_json.get("custom_metadata")
        else:
            # tags and custom_metadata are not propagated if multiple data
            # assets are provided
            data_asset_name = None
            data_asset_tags = []
            data_asset_custom_metadata = {}

        # 2. run capsule
        logger.info("Running capsule")
        run_capsule_response = self._run_capsule(
            self.job_config.run_capsule_config,
            input_data_assets=input_data_assets,
        )
        responses["run"] = run_capsule_response

        # 3. capture results (optional)
        if self.job_config.capture_result_config is not None:
            logger.info("Capturing results")
            # if the data asset was registered, then the input asset name will
            # be the same as the new registered data asset name
            if self.job_config.register_config is not None:
                input_data_asset_name = (
                    self.job_config.register_config.asset_name
                )
            elif self.job_config.capture_result_config.asset_name is None:
                assert data_asset_name is not None, (
                    "If a data asset was not registered and the job used "
                    "more than one data asset, then the input_data_asset_name "
                    "must be provided."
                )
                input_data_asset_name = data_asset_name
            else:
                input_data_asset_name = (
                    self.job_config.capture_result_config.asset_name
                )
            capture_result_response = self._capture_result(
                computation_id=run_capsule_response.json()["id"],
                input_data_asset_name=input_data_asset_name,
                additional_tags=data_asset_tags,
                additional_custom_metadata=data_asset_custom_metadata,
                capture_result_config=self.job_config.capture_result_config,
            )
            responses["capture"] = capture_result_response

        return responses
