# BlobServiceManager class
import codecs
import os
from typing import List, Any, Dict
from datetime import datetime, timedelta, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
    BlobClient,
    BlobLeaseClient,
    ContainerClient
)
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, ClientAuthenticationError

from src.common.exceptions import BlobStorageException
from src.common.logger import logger
from src.ingestion.models import BlobStorageConfig


class BlobServiceManager:
    def __init__(self, service_config: BlobStorageConfig):
        self._credential = DefaultAzureCredential()
        self._blob_service_client = BlobServiceClient(account_url=service_config.account_url, credential=self._credential)
        self.container_name = service_config.container_name
        self.account_name = self._blob_service_client.account_name
        self._container_client = self.get_container_client(container_name=self.container_name)

        logger.info("[BlobServiceManager] Blob Service client initialized for account: '%s'.", self.account_name)

    def get_container_client(self, container_name: str = '') -> ContainerClient:
        if not container_name:
            container_name = self.container_name

        container_client = self._blob_service_client.get_container_client(container_name)
        logger.info("[BlobServiceManager] Container client initialized for container: %s", container_client.url)
        return container_client

    def get_blob_client(self, blob_name: str, container_name: str = '') -> BlobClient:
        if not container_name:
            container_name = self.container_name

        blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        logger.debug(f"[ContainerManager] Blob client initialized for blob: '%s'", blob_client.blob_name)
        return blob_client

    def get_service_properties(self) -> Dict[str, Any]:
        return self._blob_service_client.get_service_properties()

    def list_blobs(self, container: ContainerClient = None, path: str = '') -> List[str]:
        """List PDF blobs in source folder."""
        try:
            if not container.exists():
                container = self._container_client

            blobs_list = container.list_blobs(name_starts_with=path, include='metadata')
            blobs = list(blobs_list)
            logger.info("[BlobServiceManager] Retrieved blobs list from container: '%s'.", len(list(blobs)))

            for blob in blobs:
                logger.info("[BlobServiceManager] Retrieved (metadata: %s): %s", str(blob.metadata), blob.name)

            return [blob.name for blob in blobs]

        except (ResourceNotFoundError, ClientAuthenticationError) as e:
            logger.error("[TransferManager] Failed to list PDF blobs in source folder: '%s'.", str(e))
            raise BlobStorageException(f"[BlobServiceManager] Failed to list PDF blobs in source folder: {str(e)}.")

    def list_containers(self, name_starts_with: str = '') -> List[str]:
        containers = self._blob_service_client.list_containers(name_starts_with=name_starts_with)
        return [container.name for container in containers]

    def generate_user_delegation_sas(self, blob_name: str) -> str:
        """Generate a user delegation SAS token for a blob."""
        try:
            delegation_key_start_time = datetime.now(timezone.utc)
            delegation_key_expiry_time = delegation_key_start_time + timedelta(hours=2)
            key = self._blob_service_client.get_user_delegation_key(
                key_start_time=delegation_key_start_time,
                key_expiry_time=delegation_key_expiry_time
            )

            sas_token = generate_blob_sas(
                account_name=self.account_name,
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
            logger.error("[TransferManager] Failed to generate user delegation SAS token: '%s'.", str(e))
            raise BlobStorageException(f"[BlobServiceManager] Failed to generate SAS token: {str(e)}")

    def copy_blob(self, source_blob: BlobClient, target_blob: BlobClient) -> None:
        """Copy a blob from source to target using a lease to prevent modifications during the copy."""
        lease = BlobLeaseClient(client=source_blob)
        try:
            # Acquire a lease on the source blob
            lease.acquire(lease_duration=-1)  # Infinite lease

            # Generate SAS token for the source blob
            sas_token = self.generate_user_delegation_sas(blob_name=source_blob.blob_name)
            source_blob_sas_url = source_blob.url + "?" + sas_token

            # Start the copy operation
            copy_operation = target_blob.start_copy_from_url(source_url=source_blob_sas_url, requires_sync=False)
            logger.info(
                f"Started copy operation for blob '{source_blob.blob_name}'. Status: {copy_operation['status']}")

        except Exception as e:
            logger.error(f"Failed to copy blob '{source_blob.blob_name}': {str(e)}")
            raise BlobStorageException(f"Failed to copy blob '{source_blob.blob_name}': {str(e)}")
        finally:
            # Release the lease
            lease.release()

