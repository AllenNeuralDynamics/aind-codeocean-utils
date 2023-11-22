"""Module with generic Code Ocean job"""
import time
from copy import deepcopy
from typing import Dict, List, Optional

from aind_codeocean_api.codeocean import CodeOceanClient


class CodeOceanJob:
    """This class contains convenient methods that any Capsule Job can use.
    Any child class needs to implement the run_job method."""

    def __init__(
        self, 
        co_client: CodeOceanClient, 
        capsule_or_pipeline_id: str,
        data_assets: Optional[List[Dict]] = None,
        run_parameters: Optional[List] = None,
    ):
        """
        CapsuleJob class constructor.

        Parameters
        ----------
        configs : dict
          Configuration parameters of the capsule job.
        co_client : CodeOceanClient
          A client that can be used to interface with the Code Ocean API.
        """
        self.co_client = co_client
        self.capsule_or_pipeline_id = capsule_or_pipeline_id
        self._data_assets = data_assets
        self._run_parameters = run_parameters

    @property
    def data_assets(self):
        return self._data_assets

    @data_assets.setter
    def data_assets(self, data_assets: List[Dict]):
        self._data_assets = data_assets

    @property
    def run_parameters(self):
        return self._run_parameters

    @run_parameters.setter
    def run_parameters(self, run_parameters: List):
        self._run_parameters = run_parameters

    def wait_for_data_availability(
        self,
        data_asset_id: str,
        timeout_seconds: int = 300,
        pause_interval=10,
    ):
        """
        There is a lag between when a register data request is made and when the
        data is available to be used in a capsule.
        Parameters
        ----------
        data_asset_id : str
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
        capsule_version: Optional[int] = None,
        pause_interval: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ):
        """
        Run a specified capsule with the given data assets. If the
        pause_interval is set, the method will return until the capsule run is
        finished before returning a response. If pause_interval is set, then
        the timeout_seconds can also optionally be set to set a max wait time.
        Parameters
        ----------
        capsule_id : str
          ID of the Code Ocean capsule to be run
        data_assets : List[Dict]
          List of data assets for the capsule to run against. The dict should
          have the keys id and mount.
        capsule_version : Optional[int]
          Run a specific version of the capsule to be run
        pause_interval : Optional[int]
          How often to check if the capsule run is finished.
        timeout_seconds : Optional[int]
          If pause_interval is set, the max wait time to check if the capsule
          is finished.

        Returns
        -------

        """
        run_capsule_response = self.co_client.run_capsule(
            capsule_id=self.capsule_or_pipeline_id, data_assets=self.data_assets, version=capsule_version,
            parameters=self.run_parameters
        )
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

    # TODO or move this elsewhere
    # def register_data(
    #     self,
    #     asset_name: str,
    #     mount: str,
    #     bucket: str,
    #     prefix: str,
    #     access_key_id: Optional[str],
    #     secret_access_key: Optional[str],
    #     tags: List[str],
    #     custom_metadata: Optional[dict] = None,
    #     viewable_to_everyone=False,
    # ):
    #     """
    #     Register a data asset. Can also optionally update the permissions on
    #     the data asset.
    #     Parameters
    #     ----------
    #     asset_name : str
    #       The name to give the data asset
    #     mount : str
    #       The mount folder name
    #     bucket : str
    #       The s3 bucket the data asset is located.
    #     prefix : str
    #       The s3 prefix where the data asset is located.
    #     access_key_id : Optional[str]
    #       The aws access key to access the bucket/prefix
    #     secret_access_key : Optional[str]
    #       The aws secret access key to access the bucket/prefix
    #     tags : List[str]
    #       The tags to use to describe the data asset
    #     custom_metadata : Optional[dict]
    #         What key:value metadata tags to apply to the asset.
    #     viewable_to_everyone : bool
    #       If set to true, then the data asset will be shared with everyone.
    #       Default is false.

    #     Returns
    #     -------
    #     requests.Response

    #     """
    #     data_asset_reg_response = self.co_client.register_data_asset(
    #         asset_name=asset_name,
    #         mount=mount,
    #         bucket=bucket,
    #         prefix=prefix,
    #         access_key_id=access_key_id,
    #         secret_access_key=secret_access_key,
    #         tags=tags,
    #         custom_metadata=custom_metadata,
    #     )

    #     if viewable_to_everyone:
    #         response_contents = data_asset_reg_response.json()
    #         data_asset_id = response_contents["id"]
    #         response_data_available = self.wait_for_data_availability(data_asset_id)

    #         if response_data_available.status_code != 200:
    #             raise FileNotFoundError(f"Unable to find: {data_asset_id}")

    #         # Make data asset viewable to everyone
    #         update_data_perm_response = self.co_client.update_permissions(
    #             data_asset_id=data_asset_id, everyone="viewer"
    #         )
    #         logger.info(
    #             f"Permissions response: {update_data_perm_response.status_code}"
    #         )

    #     return data_asset_reg_response

    def capture_result(
        self,
        computation_id: str,
        asset_name: str,
        mount: str,
        tags: List[str],
        custom_metadata: Optional[dict] = None,
        viewable_to_everyone: bool = False,
    ):
        """
        Capture a result as a data asset. Can also share it with everyone.
        Parameters
        ----------
        computation_id : str
          ID of the computation
        asset_name : str
          Name to give the data asset
        mount : str
          Mount folder name for the data asset.
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
        print(asset_name)
        reg_result_response = self.co_client.register_result_as_data_asset(
            computation_id=computation_id,
            asset_name=asset_name,
            mount=mount,
            tags=tags,
            custom_metadata=custom_metadata,
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
            print(f"Updating permissions {update_res_perm_response.status_code}")
        return reg_result_response

    def run_job(
        self,
        pause_interval: int = 300,
        run_capsule_config : dict = {},
        capture_results : bool = True,
        capture_result_config : dict = {},
        capture_results_asset_name : Optional[str] = None,
        capture_results_mount : Optional[str] = None,
    ):
        """
        Method to run the pipeline job
        """
        # 1. check data assets exist!
        for data_asset in self.data_assets:
            data_asset_id = data_asset["id"]
            response = self.co_client.get_data_asset(data_asset_id)
            response_json = response.json()
            if "message" in response_json and "not found" in response_json["message"]:
                raise FileNotFoundError(f"Unable to find: {data_asset_id}")


        # 2. run capsule
        if "pause_interval" not in run_capsule_config:
            run_capsule_config["pause_interval"] = pause_interval
        run_capsule_response = self.run_capsule(**run_capsule_config)
        computation_id = run_capsule_response.json()["id"]

        # 3. capture results
        if capture_results:
            assert capture_results_asset_name is not None
            assert capture_results_mount is not None
            if "asset_name" not in capture_result_config:
                capture_result_config["asset_name"] = capture_results_asset_name
            if "mount" not in capture_result_config:
                capture_result_config["mount"] = capture_results_mount
            assert "computation_id" not in capture_result_config
            capture_result_config["computation_id"] = computation_id
            capture_results_response = self.capture_result(**capture_result_config)
        else:
            capture_results_response = None
        return run_capsule_response, capture_results_response

            