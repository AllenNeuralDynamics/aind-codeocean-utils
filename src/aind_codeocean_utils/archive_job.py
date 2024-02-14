from aind_codeocean_api.codeocean import CodeOceanClient
from datetime import datetime
import logging

def delete_archived_data_assets(client, keep_after: datetime, dry_run: bool = True):
    assets = client.search_all_data_assets(archived=True).json()['results']

    assets_to_delete = []

    for asset in assets:
        created = datetime.fromtimestamp(asset['created'])
        last_used = datetime.fromtimestamp(asset['last_used']) if asset['last_used'] != 0 else None

        old = created < keep_after
        not_used_recently = not last_used or last_used < keep_after
        
        if old and not_used_recently:
            assets_to_delete.append(asset)

    external_size = 0
    internal_size = 0
    for asset in assets_to_delete:
        size = asset.get('size', 0)
        is_external = 'sourceBucket' in asset
        if is_external:        
            external_size += size
        else:
            internal_size += size
        logging.info(f"{asset['name']} ({'external' if is_external else 'internal'})")
                    
    logging.info(f"deleting {len(assets)} archived assets, {internal_size / 1e9} GBs internal, {external_size / 1e9} GBs external")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from aind_codeocean_api.credentials import CodeOceanCredentials
    from datetime import timedelta
    creds = CodeOceanCredentials()
    client = CodeOceanClient.from_credentials(creds)    

    assets_to_delete = delete_archived_data_assets(client, keep_after=datetime.now() - timedelta(days=30), dry_run=True)