# TransferManager class
from azure.core.exceptions import ClientAuthenticationError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from src.common.exceptions import TransferError
from src.ingestion.blob_manager import BlobServiceManager
from src.ingestion.file_handler import FileHandler


class TransferManager:
    def __init__(self, source_manager: BlobServiceManager, target_manager: BlobServiceManager):
        self.source_manager = source_manager
        self.target_manager = target_manager

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((ClientAuthenticationError, TransferError)),
    )
    def transfer_files(self, config: dict) -> None:
        try:
            account_name = self.source_manager.account_name
            container_name = self.source_manager.container_name

            blobs = self.source_manager.list_blobs(source_folder_path=config['source_folder_path'])
            filtered_files = FileHandler.filter_files_by_name(files=blobs, filter_string=config['filter_string'])

            for blob_name in filtered_files:
                try:
                    sas_token = self.source_manager.generate_user_delegation_sas(blob_name=blob_name)
                    source_blob_url = \
                        f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

                    folder_ref = blob_name.split("/")[-2].split("_")[-1]
                    new_name = FileHandler.rename(folder_ref=folder_ref)
                    target_blob_path = f"{config['target_folder_path']}/{new_name}"

                    target_blob_client = self.target_manager.blob_service_client.get_blob_client(
                        container=config["target_container_name"],
                        blob=target_blob_path
                    )

                    copy_operation = target_blob_client.start_copy_from_url(
                        source_url=source_blob_url,
                        requires_sync=False
                    )
                    print(
                        f"Started copying blob '{blob_name}' from '{container_name}' to '{config['target_container_name']}'.")

                except Exception as e:
                    raise TransferError(f"Error transferring file '{str(blob_name)}': {str(e)}")

        except Exception as e:
            raise TransferError(f"Files transfer operation failed.': {str(e)}")
