from typing import List, Tuple

import requests
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import (
    ComputationDataAsset,
    RunCapsuleRequest,
)
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest,
)
from aind_data_schema.core.data_description import DataLevel


def build_processed_data_asset_name(input_data_asset_name, process_name):
    capture_time = datetime_to_name_string(datetime.now())

    return f"{input_data_asset_name}" f"_{process_name}" f"_{capture_time}"


def add_data_level_metadata(
    data_level: DataLevel,
    tags: List[str] = None,
    custom_metadata: dict = None,
) -> Tuple[List[str], dict]:
    """Add data level metadata to a data asset."""

    tags = set(tags or [])
    tags.add(data_level.value)
    tags = list(tags)

    if data_level == DataLevel.DERIVED:
        tags.discard(DataLevel.RAW.value)

    custom_metadata = custom_metadata or {}
    custom_metadata.update({"data level": data_level.value})

    return tags, custom_metadata


class CodeOceanJob:
    def __init__(
        self,
        co_client: CodeOceanClient,
        register_data_config: CreateDataAssetRequest = None,
        process_config: RunCapsuleRequest = None,
        capture_result_config: CreateDataAssetRequest = None,
        assets_viewable_to_everyone: bool = True,
        process_poll_interval_seconds: int = 300,
        process_timeout_seconds: int = None,
        add_data_level_tags: bool = True,
    ):
        self.co_client = co_client
        self.register_data_config = register_data_config
        self.process_config = process_config
        self.capture_result_config = (capture_result_config,)
        self.assets_viewable_to_everyone = assets_viewable_to_everyone
        self.add_data_level_tags = add_data_level_tags

    def run_job(self):
        """Run the job."""

        registered_data_asset = None

        if self.register_data_config:
            if self.add_data_level_tags:
                tags, custom_metadata = add_data_level_metadata(
                    DataLevel.RAW,
                    self.register_data_config.tags,
                    self.register_data_config.custom_metadata,
                )
                self.register_data_config.tags = tags
                self.register_data_config.custom_metadata = custom_metadata

            register_data_reponse = self.register_data_and_update_permissions(
                register_data_config=self.register_data_config
            )
            register_data_response_json = register_data_response.json()

            registered_data_asset = ComputationDataAsset(
                id=register_data_asset_response_json["id"],
                mount=self.register_data_config.mount,
            )

        if self.process_config:
            self.process_config.data_assets.append(registered_data_asset)

            process_response = self.process_and_wait_for_result(
                self.process_config
            )

        if self.capture_result_config:
            self.capture_result_config.source = Source(
                computation=Sources.Computation(
                    id=process_response.json()["id"]
                )
            )

            if (
                self.capture_result_config.asset_name is None
                and self.register_data_config
            ):
                # If the asset name is not provided, build it from the input data asset name
                # TODO: support asset names from processed results

                self.capture_result_config.asset_name = (
                    build_processed_data_asset_name(
                        self.register_data_config.asset_name,
                        capture_result_config.process_name,
                    )
                )

            if self.add_data_level_tags:
                tags, custom_metadata = add_data_level_metadata(
                    DataLevel.DERIVED,
                    self.capture_result_config.tags,
                    self.capture_result_config.custom_metadata,
                )
                self.capture_result_config.tags = tags
                self.capture_result_config.custom_metadata = custom_metadata

            capture_result_reponse = self.register_data_and_update_permissions(
                register_data_config=self.capture_result_config
            )

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

    def register_data_and_update_permissions(
        self, register_data_config: CreateDataAssetRequest
    ) -> requests.Response:
        """
        Register a data asset. Can also optionally update the permissions on
        the data asset.

        Parameters
        ----------
        register_data_config : CreateDataAssetRequest

        Notes
        -----
        The credentials for the s3 bucket must be set in the environment.

        Returns
        -------
        requests.Response
        """

        # TODO handle non-aws sources
        assert (
            register_config.source.aws.keep_on_external_storage is True
        ), "AWS data assets must be kept on external storage."

        data_asset_reg_response = self.co_client.create_data_asset(
            create_data_asset_request
        )
        data_asset_reg_response_json = data_asset_reg_response.json()

        if data_asset_reg_response_json.get("id") is None:
            raise KeyError(
                f"Something went wrong registering"
                f" {asset_name}. "
                f"Response Status Code: {data_asset_reg_response.status_code}. "
                f"Response Message: {data_asset_reg_response_json}"
            )

        if self.assets_viewable_to_everyone:
            data_asset_id = data_asset_reg_response_json["id"]
            response_data_available = self_wait_for_data_availability(
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

    def process_and_wait_for_result(
        self, process_config: RunCapsuleRequest
    ) -> requests.Response:

        self.check_data_assets(process_config.data_assets)

        # TODO: Handle case of bad response from code ocean
        run_capsule_response = self.co_client.run_capsule(run_capsule_request)
        run_capsule_response_json = run_capsule_response.json()
        computation_id = run_capsule_response_json["id"]

        # TODO: We may need to clean up the loop termination logic
        if self.process_poll_interval_seconds:
            executing = True
            num_checks = 0
            while executing:
                num_checks += 1
                time.sleep(self.process_poll_interval_seconds)
                computation_response = self.co_client.get_computation(
                    computation_id
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


if __name__ == "__main__":
    pass
