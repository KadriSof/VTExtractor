# Main ingestion logic
from dotenv import load_dotenv

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.ingestion.blob_manager import BlobServiceManager
from src.ingestion.transfer_manager import TransferManager
from src.ingestion.models import BlobStorageConfig, TransferConfig
# from src.common.logger import logger

load_dotenv()


class IngestionService:
    def __init__(self, source_config: BlobStorageConfig, target_config: BlobStorageConfig):
        credential = DefaultAzureCredential()

        self.source_manager = BlobServiceManager(
            blob_service_client=BlobServiceClient(
                account_url=source_config.account_url, credential=credential
            ),
            container_name=source_config.container_name,
        )
        self.target_manager = BlobServiceManager(
            blob_service_client=BlobServiceClient(
                account_url=target_config.account_url, credential=credential
            ),
            container_name=target_config.container_name,
        )
        self.transfer_manager = TransferManager(source_manager=self.source_manager, target_manager=self.target_manager)

    def ingest_files(self, transfer_config: TransferConfig) -> None:
        """Ingest files from source to target storage"""
        # logger.info("Fetching PDFs from Azure Blob Storage...")
        self.transfer_manager.transfer_files(config=transfer_config.model_dump())
        # logger.info("File ingestion completed.)"
