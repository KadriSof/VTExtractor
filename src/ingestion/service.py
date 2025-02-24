# Main ingestion logic
from dotenv import load_dotenv

from src.common.storage_manager import BlobServiceManager
from src.ingestion.transfer_manager import TransferManager
from src.ingestion.models import BlobStorageConfig, TransferConfig
from src.common.logger import logger

load_dotenv()


class IngestionService:
    def __init__(self, source_config: BlobStorageConfig, target_config: BlobStorageConfig):

        # self._source_credential = AzureNamedKeyCredential(name=source_config.account_name, key=source_config.key)
        # self._target_credential = AzureNamedKeyCredential(name=target_config.account_name, key=target_config.key)

        self.source_manager = BlobServiceManager(service_config=source_config)
        self.target_manager = BlobServiceManager(service_config=target_config)
        self.transfer_manager = TransferManager(source_manager=self.source_manager, target_manager=self.target_manager)
        logger.info(f"[IngestionService] Ingestion service initialized.")

    def ingest_files(self, transfer_config: TransferConfig) -> None:
        """Ingest files from source to target storage"""
        logger.info("[IngestionService] Fetching PDFs from Azure Blob Storage...")
        try:
            self.transfer_manager.transfer_files_refactored(config=transfer_config)
            logger.info("[IngestionService] File ingestion completed successfully.")

        except Exception as e:
            logger.error("[IngestionService] File ingestion failed: %s", str(e))
            raise e

    def list_target_files(self, target_config: BlobStorageConfig):
        container = self.target_manager.get_container_client(container_name=target_config.container_name)
        logger.info(f"[IngestionService] Listing files from target container: '%s", container.url)
        self.source_manager.list_blobs(container=container)

    def list_source_files(self, source_config: BlobStorageConfig):
        container = self.source_manager.get_container_client(container_name=source_config.container_name)
        logger.info(f"[IngestionService] Listing files from source container: '%s", container.url)
        self.source_manager.list_blobs(container=container, path="bronze/poland/english/komornik/document")
