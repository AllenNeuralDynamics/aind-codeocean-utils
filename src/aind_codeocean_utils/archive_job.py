from aind_codeocean_api.codeocean import CodeOceanClient
from datetime import timedelta

class ArchiveDeleteJob:
    """
    This class contains convenient methods to register data assets,
    run capsules, and capture results.
    """

    def __init__(self, co_client: CodeOceanClient):
        self.client = co_client

    def find_archived_data_assets(self, minimum_age: timedelta):
        pass

if __name__ == __main__:
    from aind_codeocean_api.credentials import CodeOceanCredentials
    creds = CodeOceanCredentials()
    client = CodeOceanClient.from_credentials(cred)
    j = ArchiveDeleteJob(client)

    jobs = j.find_archived_data_assets(minimum_age=timedelta(days=30))