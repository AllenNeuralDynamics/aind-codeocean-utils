"""Module of classes to handle interfacing with the Code Ocean index."""

import logging
from datetime import datetime
from typing import Dict, Iterator, List, Optional

from aind_codeocean_api.codeocean import CodeOceanClient
from botocore.client import BaseClient


class APIHandler:
    """Class to handle common tasks modifying the Code Ocean index."""

    def __init__(
        self,
        co_client: CodeOceanClient,
        s3: Optional[BaseClient] = None,
        dryrun: bool = False,
    ):
        """
        Class constructor
        Parameters
        ----------
        co_client : CodeOceanClient
        s3 : Optional[BaseClient]
          For operations that require communicating with AWS S3, a user will
          need to create a boto.client('s3') object. Default is None.
        dryrun : bool
          Perform a dryrun of the operations without actually making any
          changes to the index. Default is False.
        """
        self.co_client = co_client
        self.s3 = s3
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

    def find_archived_data_assets_to_delete(
        self, keep_after: datetime
    ) -> List[dict]:
        """
        Find archived data assets which were last used before the keep_after
        datetime.
        Parameters
        ----------
        keep_after : datetime
          Archived data assets have a last_used field

        Returns
        -------
        List[dict]
          A list of data assets objects

        """

        assets = self.co_client.search_all_data_assets(archived=True).json()[
            "results"
        ]

        assets_to_delete = []

        for asset in assets:
            created = datetime.fromtimestamp(asset["created"])
            last_used = (
                datetime.fromtimestamp(asset["last_used"])
                if asset["last_used"] != 0
                else None
            )

            old = created < keep_after
            not_used_recently = not last_used or last_used < keep_after

            if old and not_used_recently:
                assets_to_delete.append(asset)

        external_count = 0
        external_size = 0

        internal_count = 0
        internal_size = 0
        for asset in assets_to_delete:
            size = asset.get("size", 0)
            is_external = "sourceBucket" in asset
            if is_external:
                external_count += 1
                external_size += size
            else:
                internal_count += 1
                internal_size += size
            logging.info(f"name: {asset['name']}, type: {asset['type']}")

        logging.info(
            f"{len(assets_to_delete)}/{len(assets)} archived assets deletable"
        )
        logging.info(
            f"internal: {internal_count} assets, {internal_size / 1e9} GBs"
        )
        logging.info(
            f"external: {external_count} assets, {external_size / 1e9} GBs"
        )

        return assets_to_delete

    def find_external_data_assets(self) -> Iterator[dict]:
        """
        Find external data assets by checking if the data asset responses
        from CodeOcean have a source bucket and are of type 'dataset'.
        Returns
        -------
        Iterator[dict]
          An iterator of data assets objects

        """

        response = self.co_client.search_all_data_assets(type="dataset")
        assets = response.json()["results"]

        for asset in assets:
            bucket = asset.get("sourceBucket", {}).get("bucket", None)
            if bucket:
                yield asset

    def find_nonexistent_external_data_assets(self) -> Iterator[dict]:
        """
        Find external data assets that do not exist in S3. Makes a call
        to CodeOcean and returns an iterator over external data assets.
        Steps through each external data asset and queries for the existence
        in S3. If it exists or an error occurs while querying S3, then the
        data asset will not be added to the return response.
        Returns
        -------
        Iterator[dict]
           An iterator of data asset objects.

        """

        for asset in self.find_external_data_assets():
            sb = asset["sourceBucket"]

            try:
                exists = self._bucket_prefix_exists(sb["bucket"], sb["prefix"])
                logging.debug(
                    f"{sb['bucket']} {sb['prefix']} exists? {exists}"
                )
                if not exists:
                    yield asset
            except Exception as e:
                logging.error(e)

    def _bucket_prefix_exists(self, bucket: str, prefix: str) -> bool:
        """
        Check if bucket/prefix exists in S3. Prefix could be empty string.
        Parameters
        ----------
        bucket : str
          S3 bucket
        prefix : str
          S3 object prefix

        Returns
        -------
        bool
          True if bucket and prefix exists in S3. False otherwise.

        """

        prefix = prefix.rstrip("/")
        resp = self.s3.list_objects(
            Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
        )
        return "CommonPrefixes" in resp
