"""Module of classes to handle interfacing with the Code Ocean index."""

import logging
from typing import Dict, Iterator, List, Optional

from aind_codeocean_api.codeocean import CodeOceanClient


class APIHandler:
    """Class to handle common tasks modifying the Code Ocean index."""

    def __init__(self, co_client: CodeOceanClient, dryrun: bool = False):
        """
        Class constructor
        Parameters
        ----------
        co_client : CodeOceanClient
        dryrun : bool
          Perform a dryrun of the operations without actually making any
          changes to the index.
        """
        self.co_client = co_client
        self.dryrun = dryrun

    def update_tags(
        self,
        tags_to_remove: Optional[List[str]] = None,
        tags_to_add: Optional[List[str]] = None,
        tags_to_replace: Optional[Dict[str, str]] = None,
        data_assets=Iterator[dict],
    ) -> None:
        """
        Updates tags for a list of data assets. Will first remove tags in the
        tags_to_remove list if they exist, and then add the tags_to_add. Will
        keep the tags already on the data asset if they are not explicitly set
        in the tags_to_remove list. Will use tags_to_replace dictionary to
        replace tags directly. Note, the tags_to_replace will be performed
        after tags_to_remove and tags_to_add if those are not None.
        Parameters
        ----------
        tags_to_remove : Optional[List[str]]
          Optional list of tags to remove from a data asset
        tags_to_add: Optional[List[str]]
          Optional list of tags to add to a data asset
        tags_to_replace: Optional[Dict[str, str]]
          Optional dictionary of tags to replace. For example,
          {"old_tag0": "new_tag0", "old_tag1": "new_tag1"}.
        data_assets : Iterator[dict]
          An iterator of data assets. The shape of the response is described
          at:
          "https://docs.codeocean.com
          /user-guide/code-ocean-api/swagger-documentation"
          The relevant fields are id: str, name: str, and tags: list[str].

        Returns
        -------
        None
          Sends the requests and logs the responses.

        """
        # Remove tags that are in tags_to_remove and then add tags
        # that are in tags_to_add
        tags_to_add = set() if tags_to_add is None else tags_to_add
        tags_to_remove = set() if tags_to_remove is None else tags_to_remove
        tags_to_replace = (
            dict() if tags_to_replace is None else tags_to_replace
        )
        for data_asset in data_assets:
            # Remove tags in tags_to_remove
            tags = (
                set()
                if data_asset.get("tags") is None
                else set(data_asset["tags"])
            )
            tags.difference_update(tags_to_remove)
            tags.update(tags_to_add)
            mapped_tags = {tags_to_replace.get(tag, tag) for tag in tags}
            data_asset_id = data_asset["id"]
            data_asset_name = data_asset["name"]
            logging.debug(f"Updating data asset: {data_asset}")
            # new_name is a required field, we can set it to the original name
            if self.dryrun is True:
                logging.info(
                    f"(dryrun): "
                    f"co_client.update_data_asset("
                    f"data_asset_id={data_asset_id},"
                    f"new_name={data_asset_name},"
                    f"new_tags={mapped_tags},)"
                )
            else:
                response = self.co_client.update_data_asset(
                    data_asset_id=data_asset_id,
                    new_name=data_asset_name,
                    new_tags=list(mapped_tags),
                )
                logging.info(response.json())
