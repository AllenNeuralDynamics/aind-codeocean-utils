from pathlib import Path
from typing import Iterator, List, Optional

from aind_codeocean_api.codeocean import CodeOceanClient
from requests import Response


class APIHandler:
    def __init__(
        self, co_client: CodeOceanClient, log_file: Optional[Path] = None
    ):
        self.co_client = co_client
        self.log_file = log_file

    def update_tags(
        self,
        tags_to_remove: Optional[List[str]] = None,
        tags_to_add: Optional[List[str]] = None,
        data_assets=Iterator[dict],
    ) -> Iterator[Response]:
        """
        Updates tags for a list of data assets. Will first remove tags in the
        tags_to_remove list if they exist, and then add the tags_to_add. Will
        keep the tags already on the data asset if they are not explicitly set
        in the tags_to_remove list. Will change every data asset in
        data_assets.
        Parameters
        ----------
        tags_to_remove : Optional[List[str]]
          Optional list of tags to remove from a data asset
        tags_to_add: Optional[List[str]]
          Optional list of tags to add to a data asset
        data_assets : Iterator[dict]
          An iterator of data assets. The shape of the response is described
          at:
          "https://docs.codeocean.com
          /user-guide/code-ocean-api/swagger-documentation"
          The relevant fields are id: str, name: str, and tags: list[str].

        Returns
        -------
        Iterator[Response]
          An iterator of the responses from the update metadata requests.

        """

        for data_asset in data_assets:
            filtered_tags = [
                tag for tag in data_asset["tags"] if tag not in tags_to_remove
            ]
            complete_tags = list(set(tags_to_add + filtered_tags))
            data_asset_id = data_asset["id"]
            data_asset_name = data_asset["name"]
            # new_name is a required field, we can set it to the original name
            response = self.co_client.update_data_asset(
                data_asset_id=data_asset_id,
                new_name=data_asset_name,
                new_tags=complete_tags,
            )
            yield response
