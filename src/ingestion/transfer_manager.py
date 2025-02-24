# TransferManager class
from azure.core.exceptions import ClientAuthenticationError
from azure.storage.blob import BlobLeaseClient, BlobClient
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from src.common.exceptions import TransferError
from src.common.logger import logger
from src.common.storage_manager import BlobServiceManager
from src.ingestion.file_handler import FileHandler
from src.ingestion.models import TransferConfig


class TransferManager:
    def __init__(self, source_manager: BlobServiceManager, target_manager: BlobServiceManager):
        self.source_manager = source_manager
        self.target_manager = target_manager
        logger.info("[TransferManager] TransferManager initialized")

    @staticmethod
    def _is_blob_copied(blob_client: BlobClient) -> bool:
        """Checks if a blob has already been copied."""
        logger.debug(f"[TransferManager] Checking if blob '{blob_client.blob_name}' has been copied.")
        metadata = blob_client.get_blob_properties().metadata
        return metadata.get("copied", "false") == "true"

    @staticmethod
    def _mark_blob_as_copied(blob_client: BlobClient) -> None:
        """Marks a blob as copied."""
        logger.debug(f"[TransferManager] Marking blob '{blob_client.blob_name}' as copied.")
        blob_client.set_blob_metadata(metadata={"copied": "true"})
        blob_client.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((ClientAuthenticationError, TransferError)),
    )
    def _transfer_blob(self, source_blob_name: str, target_blob_name: str, config: TransferConfig) -> None:
        """Transfer a single blob with retry logic."""
        try:
            source_blob_client = self.source_manager.get_blob_client(
                blob_name=source_blob_name,
                container_name=config.source_container_name
            )
            target_blob_client = self.target_manager.get_blob_client(
                blob_name=target_blob_name
            )

            lease = BlobLeaseClient(client=source_blob_client)
            sas_token = self.source_manager.generate_user_delegation_sas(blob_name=source_blob_name)
            source_blob_sas_url = source_blob_client.url + "?" + sas_token
            lease.acquire(lease_duration=-1)

            copy_operation = dict()
            copy_operation = target_blob_client.start_copy_from_url(source_url=source_blob_sas_url, requires_sync=False)
            copy_status = target_blob_client.get_blob_properties().copy.status
            lease.break_lease()

            logger.info(
                "[TransferManager] Started copying file '%s' - status: %s",
                source_blob_name,
                copy_status,
            )

            if copy_status != 'failed' or copy_status != 'aborted':
                self._mark_blob_as_copied(blob_client=source_blob_client)

        except Exception as e:
            logger.error("[TransferManager] Error transferring file '%s': %s", source_blob_name, str(e))
            raise TransferError("[TransferManager] Error transferring file '%s': %s", source_blob_name, str(e))

    def transfer_files_refactored(self, config: TransferConfig) -> None:
        """Transfer files from the source container to the target container."""
        try:

            logger.info(f"[TransferManager] Starting files transfer...")
            source_container_client = self.source_manager.get_container_client(config.source_container_name)
            blobs_list = self.source_manager.list_blobs(container=source_container_client, path=config.source_folder_path)
            filtered_files = FileHandler.filter_files_by_name(files=blobs_list, filter_string=config.filter_string)
            logger.info("[TransferManager] Number of files to transfer: %s", len(filtered_files))

            if filtered_files:
                for blob_name in filtered_files:
                    folder_ref = blob_name.split("/")[-2].split("_")[-1]
                    target_blob_name = FileHandler.rename(folder_ref=folder_ref)
                    logger.info("[TransferManager] Starting transferring blob: '%s'", blob_name)
                    self._transfer_blob(source_blob_name=blob_name, target_blob_name=target_blob_name, config=config)
            else:
                logger.info("[TransferManager] No files to transfer.")

        except Exception as e:
            logger.error("[TransferManager] Files transfer operation failed: {}".format(str(e)))
            raise TransferError("[TransferManager] Files transfer operation failed: {}".format(str(e)))
