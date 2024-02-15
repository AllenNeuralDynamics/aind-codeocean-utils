from aind_codeocean_api.codeocean import CodeOceanClient
from datetime import datetime
import logging
import boto3
import botocore
from botocore.errorfactory import ClientError


def find_archived_data_assets_to_delete(client, keep_after: datetime):
    """find archived data assets that are safe to delete"""

    assets = client.search_all_data_assets(archived=True).json()["results"]

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

    external_size = 0
    internal_size = 0
    for asset in assets_to_delete:
        size = asset.get("size", 0)
        is_external = "sourceBucket" in asset
        if is_external:
            external_size += size
        else:
            internal_size += size
        logging.info(f"{asset['name']} {asset['type']}")

    logging.info(f"{len(assets)} archived data assets can be deleted")
    logging.info(f"{internal_size / 1e9} GBs internal")
    logging.info(f"{external_size / 1e9} GBs external")

    return assets_to_delete


def find_external_assets(client):
    """find all external data assets"""

    assets = client.search_all_data_assets(type="dataset").json()["results"]
    for asset in assets:
        bucket = asset.get("sourceBucket", {}).get("bucket", None)
        if bucket:
            yield asset


def find_nonexistant_external_data_assets(client):
    """find external data assets that do not exist"""

    s3 = boto3.client("s3")

    for asset in find_external_assets(client):
        sb = asset["sourceBucket"]

        try:
            exists = bucket_folder_exists(s3, sb["bucket"], sb["prefix"])
            logging.info(f"{sb['bucket']} {sb['prefix']} {exists}")
            if not exists:
                yield asset
        except botocore.exceptions.ClientError as e:
            logging.warning(e)


def bucket_folder_exists(s3, bucket, path) -> bool:
    """Check if folder exists. Folder could be empty."""

    path = path.rstrip("/")
    resp = s3.list_objects(
        Bucket=bucket, Prefix=path, Delimiter="/", MaxKeys=1
    )
    return "CommonPrefixes" in resp


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from aind_codeocean_api.credentials import CodeOceanCredentials
    from datetime import timedelta

    creds = CodeOceanCredentials()
    client = CodeOceanClient.from_credentials(creds)

    # assets_to_delete = find_archivable_data_assets(client, keep_after=datetime.now() - timedelta(days=30))
    find_nonexistant_external_assets(client)
