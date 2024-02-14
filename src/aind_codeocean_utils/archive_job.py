from aind_codeocean_api.codeocean import CodeOceanClient
from datetime import datetime

class ArchiveDeleteJob:
    """
    This class contains convenient methods to register data assets,
    run capsules, and capture results.
    """

    def __init__(self, co_client: CodeOceanClient):
        self.client = co_client

    def delete_data_assets(self, keep_after: datetime, dry_run: bool = True):
        assets = self.client.search_all_data_assets(archived=True).json()['results']
        

        for asset in assets:
            created = datetime.fromtimestamp(asset['created'])
            last_used = datetime.fromtimestamp(asset['last_used']) if asset['last_used'] != 0 else None

            old = created < keep_after
            not_used_recently = not last_used or last_used < keep_after
            if old and not_used_recently:
                yield asset


if __name__ == "__main__":
    from aind_codeocean_api.credentials import CodeOceanCredentials
    from datetime import timedelta
    creds = CodeOceanCredentials()
    client = CodeOceanClient.from_credentials(creds)
    j = ArchiveDeleteJob(client)

    assets_to_delete = j.find_archived_data_assets(keep_after=datetime.now() - timedelta(days=30))
    for asset in assets_to_archive:
        print(asset)