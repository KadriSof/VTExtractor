# BlobServiceManager class
from typing import List
from datetime import datetime, timedelta, timezone

from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, ClientAuthenticationError

from src.common.exceptions import BlobStorageException
from src.common.logger import logger


class TransferManager:
    def __init__(self, blob_service_client: BlobServiceClient, container_name: str):
        self.blob_service_client = blob_service_client
        self.container_name = container_name
        self._account_name = self.blob_service_client.account_name
        logger.info("[TransferManager] Blob Service client initialized for container '%s'", self.container_name)

    def list_blobs(self, source_folder_path: str) -> List[str]:
        """List PDF blobs in source folder."""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_list = container_client.list_blobs(name_starts_with=source_folder_path)
            return [blob.name for blob in blob_list if blob.name.endswith(".pdf")]

        except (ResourceNotFoundError, ClientAuthenticationError) as e:
            logger.error("[TransferManager] Failed to list PDF blobs in source folder: '%s'", str(e))
            raise BlobStorageException(f"[BlobServiceManager] Failed to list PDF blobs in source folder: {str(e)}")

    def generate_user_delegation_sas(self, blob_name: str) -> str:
        """Generate a user delegation SAS token for a blob."""
        try:
            delegation_key_start_time = datetime.now(timezone.utc)
            delegation_key_expiry_time = delegation_key_start_time + timedelta(hours=1)
            key = self.blob_service_client.get_user_delegation_key(
                key_start_time=delegation_key_start_time,
                key_expiry_time=delegation_key_expiry_time
            )

            sas_token = generate_blob_sas(
                account_name=self._account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=None,
                user_delegation_key=key,
                permission=BlobSasPermissions(read=True),
                start=delegation_key_start_time,
                expiry=delegation_key_expiry_time,
            )

            return sas_token

        except Exception as e:
            logger.error("[TransferManager] Failed to generate user delegation SAS token: '%s'", str(e))
            raise BlobStorageException(f"[BlobServiceManager] Failed to generate SAS token: {str(e)}")

    @property
    def account_name(self) -> str:
        return self._account_name
