"""Module with generic Code Ocean job"""
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import requests
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import RunCapsuleRequest
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest,
    Source,
    Sources,
)

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CodeOceanJob:
    """
    This class contains convenient methods to register data assets,
    run capsules, and capture results.
    """

    default_datetime_format = "%Y-%m-%d_%H-%M-%S"

    def __init__(self, co_client: CodeOceanClient):
        """
        CapsuleJob class constructor.

        Parameters
        ----------
        co_client : CodeOceanClient
            A client that can be used to interface with the Code Ocean API.
        """
        self.co_client = co_client

    def add_capture_time(self, name: str) -> str:
        """
        Add capture time to a name. If the name contains the string
        {capture_time}, then the current time will be appended to the
        name using the default format: "%Y-%m-%d_%H-%M-%S".
        To use a different format, use:
        {capture_time:date_time_format}, e.g. {capture_time:%Y-%m-%d}
        """
        if "{capture_time}" not in name:
            return name
        name_stem = name[: name.find("{capture_time}")]
        capture_string = name[name.find("{capture_time}"):]
        if ":" in capture_string:
            date_time_format = capture_string[
                capture_string.find(":") + 1: -1
            ]
        else:
            date_time_format = self.default_datetime_format
        capture_time = datetime.now()
        name_capture_time = (
            name_stem + f"_{datetime.strftime(capture_time, date_time_format)}"
        )
        return name_capture_time

    def wait_for_data_availability(
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

    def run_capsule(
        self,
        capsule_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        data_assets: Optional[Union[List[Dict], Tuple[Dict]]] = None,
        run_parameters: Optional[List] = None,
        pause_interval: Optional[int] = 300,
        capsule_version: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> requests.Response:
        """
        Run a specified capsule with the given data assets. If the
        pause_interval is set, the method will return until the capsule run is
        finished before returning a response. If pause_interval is set, then
        the timeout_seconds can also optionally be set to set a max wait time.

        Parameters
        ----------
        capsule_id : str
            ID of the Code Ocean capsule to be run
        pipeline_id : str
            ID of the Code Ocean pipeline to be run
        data_assets : List[Dict]
            List of data assets for the capsule to run against. The dict
            should have the keys id and mount.
        run_parameters : Optional[List]
            List of parameters to pass to the capsule.
        pause_interval : Optional[int]
            How often to check if the capsule run is finished.
            If None, then the method will return immediately without waiting
            for the computation to finish.
        capsule_version : Optional[int]
            Run a specific version of the capsule to be run
        timeout_seconds : Optional[int]
            If pause_interval is set, the max wait time to check if the
            capsule is finished.

        Returns
        -------
        requests.Response

        """
        if data_assets is not None:
            assert isinstance(
                data_assets, (list, tuple)
            ), "data_assets must be a list or tuple"
            assert all(
                [
                    "id" in data_asset and "mount" in data_asset
                    for data_asset in data_assets
                ]
            ), "data_assets must be a list of dicts with keys 'id' and 'mount'"
            # check if data assets exist
            for data_asset in data_assets:
                data_asset_id = data_asset["id"]
                response = self.co_client.get_data_asset(data_asset_id)
                response_json = response.json()
                if (
                    "message" in response_json
                    and "not found" in response_json["message"]
                ):
                    raise FileNotFoundError(f"Unable to find: {data_asset_id}")

        run_capsule_request = RunCapsuleRequest(
            capsule_id=capsule_id,
            pipeline_id=pipeline_id,
            data_assets=data_assets,
            parameters=run_parameters,
            version=capsule_version,
        )
        run_capsule_response = self.co_client.run_capsule(run_capsule_request)
        run_capsule_response_json = run_capsule_response.json()
        computation_id = run_capsule_response_json["id"]

        if pause_interval:
            executing = True
            num_checks = 0
            while executing:
                num_checks += 1
                time.sleep(pause_interval)
                curr_computation_state = self.co_client.get_computation(
                    computation_id
                ).json()

                if (curr_computation_state["state"] == "completed") or (
                    (timeout_seconds is not None)
                    and (pause_interval * num_checks >= timeout_seconds)
                ):
                    executing = False
        return run_capsule_response

    def register_data_and_update_permissions(
        self,
        asset_name: str,
        mount: str,
        bucket: str,
        prefix: str,
        public: bool = False,
        keep_on_external_storage: bool = True,
        tags: Optional[List[str]] = None,
        custom_metadata: Optional[Dict] = None,
        viewable_to_everyone=False,
    ) -> requests.Response:
        """
        Register a data asset. Can also optionally update the permissions on
        the data asset.

        Parameters
        ----------
        asset_name : str
            The name to give the data asset
        mount : str
            The mount folder name
        bucket : str
            The s3 bucket the data asset is located.
        prefix : str
            The s3 prefix where the data asset is located.
        public : bool
            Whether the data asset is public or not. Default is False.
        keep_on_external_storage : bool
            Whether to keep the data asset on external storage.
            Default is True.
        tags : List[str]
            The tags to use to describe the data asset
        custom_metadata : Optional[dict]
            What key:value metadata tags to apply to the asset.
        viewable_to_everyone : bool
            If set to true, then the data asset will be shared with everyone.
            Default is false.

        Notes
        -----
        The credentials for the s3 bucket must be set in the environment.

        Returns
        -------
        requests.Response
        """
        aws_source = Sources.AWS(
            bucket=bucket,
            prefix=prefix,
            keep_on_external_storage=keep_on_external_storage,
            public=public,
        )
        source = Source(aws=aws_source)
        create_data_asset_request = CreateDataAssetRequest(
            name=asset_name,
            tags=tags if tags is not None else [],
            mount=mount,
            source=source,
            custom_metadata=custom_metadata,
        )
        data_asset_reg_response = self.co_client.create_data_asset(
            create_data_asset_request
        )

        if viewable_to_everyone:
            response_contents = data_asset_reg_response.json()
            data_asset_id = response_contents["id"]
            response_data_available = self.wait_for_data_availability(
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

    def capture_result(
        self,
        computation_id: str,
        asset_name: str,
        mount: str,
        tags: Optional[List[str]] = None,
        custom_metadata: Optional[Dict] = None,
        viewable_to_everyone: bool = False,
    ) -> requests.Response:
        """
        Capture a result as a data asset. Can also share it with everyone.
        Parameters
        ----------
        computation_id : str
            ID of the computation
        asset_name : str
            Name to give the data asset. If the name contains the string
            {capture_time}, then the current time will be appended to the
            name using the default format: "%Y-%m-%d_%H-%M-%S"
            To use a different format, use: {capture_time:date_time_format},
            e.g. {capture_time:%Y-%m-%d}
        mount : str
            Mount folder name for the data asset. If the name contains the
            string {capture_time}, then the current time will be appended to
            the name using the default format: "%Y-%m-%d_%H-%M-%S"
            To use a different format, use: {capture_time:date_time_format},
            e.g. {capture_time:%Y-%m-%d}
        tags : List[str]
            List of tags to describe the data asset.
        custom_metadata : Optional[dict]
            What key:value metadata tags to apply to the asset.
        viewable_to_everyone : bool
            If set to true, then the data asset will be shared with everyone.
            Default is false.

        Returns
        -------
        requests.Response

        """
        # handle capture time
        asset_name = self.add_capture_time(asset_name)
        mount = self.add_capture_time(mount)

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
                f"Something went wrong registering {asset_name}. "
                f"Response Status Code: {reg_result_response.status_code}. "
                f"Response Message: {registered_results_response_json}"
            )

        results_data_asset_id = registered_results_response_json["id"]
        response_res_available = self.wait_for_data_availability(
            data_asset_id=results_data_asset_id
        )

        if response_res_available.status_code != 200:
            raise FileNotFoundError(f"Unable to find: {results_data_asset_id}")

        # Make captured results viewable to everyone
        if viewable_to_everyone:
            update_res_perm_response = self.co_client.update_permissions(
                data_asset_id=results_data_asset_id, everyone="viewer"
            )
            logger.info(
                f"Updating permissions {update_res_perm_response.status_code}"
            )
        return reg_result_response

    def run(
        self,
        capsule_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        data_assets: Optional[Union[List[Dict], Tuple[Dict]]] = None,
        run_capsule_config: dict = {},
        capture_results: bool = True,
        capture_result_config: dict = {},
    ) -> dict:
        """
        Method to run a computation and optionally capture the results.

        Parameters
        ----------
        capsule_id : str or None
            ID of the capsule to run.
        pipeline_id : str or None
            ID of the pipeline to run.
        data_assets : Optional[Union[List[Dict], Tuple[Dict]]]
            List of data assets for the capsule to run against. The dict
            should have the keys id and mount.
        run_capsule_config : dict
            Configuration parameters for running a capsule.
            Optional keys:
                * run_parameters: List[str]
                    The parameters to pass to the capsule.
                * pause_interval: int
                    How often to check if the capsule run is finished.
                    Default is 300 seconds.
                * capsule_version: int
                    Run a specific version of the capsule to be run.
                    Default is None.
                * timeout_seconds:
                    If pause_interval is set, the max wait time to check if
                    the capsule is finished. Default is None.
        capture_results : bool
            Whether to capture the results of the capsule run,
            default is True.
        capture_result_config : dict
            Configuration parameters for capturing results.
            Required keys (if capture_results is True):
                * asset_name: str
                    To add the current time to the asset name, use the
                    magic string: {capture_time} notation. The default
                    datetime format is: "%Y-%m-%d_%H-%M-%S". To use a
                    different format, use: {capture_time:date_time_format}.
                * mount: str
                    The mount folder name.
            Optional keys:
                * viewable_to_everyone: bool
                    Whether to share the captured results with everyone.
                    Default is False.
                * custom_metadata: dict
                    What key:value metadata tags to apply to the asset.
                * tags: List[str]
                    The tags to use to describe the data asset.
        """
        # 1. run capsule
        assert (
            capsule_id is not None or pipeline_id is not None
        ), "Either capsule_id or pipeline_id must be provided"
        if capsule_id is not None:
            assert (
                pipeline_id is None
            ), "If capsule_id is provided, then pipeline_id must be None"
            if "capsule_id" not in run_capsule_config:
                run_capsule_config["capsule_id"] = capsule_id
        if pipeline_id is not None:
            assert (
                capsule_id is None
            ), "If pipeline_id is provided, then capsule_id must be None"
            if "pipeline_id" not in run_capsule_config:
                run_capsule_config["pipeline_id"] = pipeline_id
        if "data_assets" not in run_capsule_config:
            run_capsule_config["data_assets"] = data_assets
        else:
            assert isinstance(
                data_assets, (list, tuple)
            ), "data_assets must be a list or tuple"
            run_capsule_config["data_assets"].extend(data_assets)

        run_capsule_response = self.run_capsule(**run_capsule_config)
        computation_id = run_capsule_response.json()["id"]

        # 2. capture results
        if capture_results:
            assert (
                "asset_name" in capture_result_config
            ), "asset_name must be provided"
            assert "mount" in capture_result_config, "mount must be provided"
            assert (
                "computation_id" not in capture_result_config
            ), "computation_id must not be provided"
            capture_result_config["computation_id"] = computation_id
            capture_results_response = self.capture_result(
                **capture_result_config
            )
        else:
            capture_results_response = None
        return dict(run=run_capsule_response, capture=capture_results_response)

    def register_and_run(
        self,
        capsule_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        register_data_config: dict = {},
        run_capsule_config: dict = {},
        additional_data_assets: Optional[List[Dict]] = None,
        capture_results: bool = True,
        capture_result_config: dict = {},
    ) -> dict:
        """
        Method to register and process data with a Code Ocean capsule/pipeline.

        Parameters
        ----------
        capsule_id : str or None
            ID of the capsule or pipeline to run.
        pipeline_id : str or None
            ID of the pipeline to run.
        register_data_config : dict
            Configuration parameters for registering data assets.
            Required keys:
                * asset_name: str
                    The name to give the data asset.
                * mount: str
                    The mount folder name.
                * bucket: str
                    The s3 bucket the data asset is located.
                * prefix: str
                    The s3 prefix where the data asset is located.
                * access_key_id: str
                    The aws access key to access the bucket/prefix.
                * secret_access_key: str
                    The aws secret access key to access the bucket/prefix.
            Optional keys:
                * tags
                * custom_metadata
        run_capsule_config : dict
            Configuration parameters for running a capsule.
            Optional keys:
                * run_parameters: List[str]
                    The parameters to pass to the capsule.
                * pause_interval: int
                    How often to check if the capsule run is finished.
                    Default is 300 seconds.
                * capsule_version: int
                    Run a specific version of the capsule to be run.
                    Default is None.
                * timeout_seconds:
                    If pause_interval is set, the max wait time to check if
                    the capsule is finished. Default is None.
        additional_data_assets : Optional[List[Dict]]
            Additional data assets to attach to the capsule run.
        capture_results : bool
            Whether to capture the results of the capsule run,
            default is True.
        capture_result_config : dict
            Configuration parameters for capturing results.
            Required keys (if capture_results is True):
                * asset_name: str
                    To add the current time to the asset name, use the
                    magic string: {capture_time} notation. The default
                    datetime format is: "%Y-%m-%d_%H-%M-%S". To use a
                    different format, use: {capture_time:date_time_format}.
                * mount: str
                    The mount folder name.
            Optional keys:
                * viewable_to_everyone: bool
                    Whether to share the captured results with everyone.
                    Default is False.
                * custom_metadata: dict
                    What key:value metadata tags to apply to the asset.
                * tags: List[str]
                    The tags to use to describe the data asset.

        Returns
        -------
        dict
            Dictionary with keys register, run, and capture.
            The values are the responses from the register, run,
            and capture requests.

        """
        # 1. register data assets
        data_asset_reg_response = self.register_data_and_update_permissions(
            **register_data_config
        )

        # 2. create data assets
        data_assets = [
            dict(
                id=data_asset_reg_response.json()["id"],
                mount=register_data_config["mount"],
            )
        ]
        if additional_data_assets is not None:
            data_assets.extend(additional_data_assets)

        # 3. process data
        responses = self.run(
            capsule_id=capsule_id,
            pipeline_id=pipeline_id,
            data_assets=data_assets,
            run_capsule_config=run_capsule_config,
            capture_results=capture_results,
            capture_result_config=capture_result_config,
        )
        responses.update(dict(register=data_asset_reg_response))
        return responses
