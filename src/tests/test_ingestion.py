import unittest
from unittest.mock import MagicMock, patch
from azure.storage.blob import BlobServiceClient
from src.ingestion.service import IngestionService
from src.ingestion.models import BlobStorageConfig, TransferConfig
from src.common.exceptions import BlobStorageException, TransferError


class TestIngestionService(unittest.TestCase):
    def setUp(self):
        # Mock configurations
        self.source_config = BlobStorageConfig(
            account_url="https://scrapers.blob.core.windows.net/",
            container_name="datalake",
        )
        self.target_config = BlobStorageConfig(
            account_url="https://auctionsdocuments.blob.core.windows.net/",
            container_name="poland-auctions-documents",
        )
        self.transfer_config = TransferConfig(
            source_container_name="datalake",
            source_folder_path="bronze/poland/english/komornik/document",
            target_container_name="poland-auctions-documents",
            target_folder_path="auction_notice_documents",
            filter_string="Treść obwieszczenia o e-licytacji (pdf)",
        )

        # Mock BlobServiceClient
        self.mock_source_client = MagicMock(spec=BlobServiceClient)
        self.mock_target_client = MagicMock(spec=BlobServiceClient)

        self.mock_credential = MagicMock()
        self.mock_source_client.credential = self.mock_credential
        self.mock_target_client.credential = self.mock_source_client

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_success(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]

        # Mock list_blobs
        self.mock_source_client.get_container_client.return_value.list_blobs.return_value = [
            MagicMock(name="file1.pdf"),
            MagicMock(name="file2.pdf"),
        ]

        # Mock generate_sas_token and start_copy
        self.mock_source_client.account_name = "source"
        self.mock_source_client.credential.account_key = "key"

        # Act
        ingestion_service = IngestionService(self.source_config, self.target_config)
        ingestion_service.ingest_files(self.transfer_config)

        # Assert
        self.mock_source_client.get_container_client.assert_called_once_with("datalake")
        self.mock_target_client.get_container_client.assert_called_once_with("poland-auctions-documents")

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_blob_storage_error(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]
        self.mock_source_client.get_container_client.return_value.list_blobs.side_effect = BlobStorageException("Failed to list blobs")

        # Act & Assert
        ingestion_service = IngestionService(self.source_config, self.target_config)
        with self.assertRaises(BlobStorageException):
            ingestion_service.ingest_files(self.transfer_config)

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_transfer_error(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]
        self.mock_source_client.get_container_client.return_value.list_blobs.return_value = [
            MagicMock(name="file1.pdf"),
        ]
        self.mock_source_client.account_name = "source"
        self.mock_source_client.credential.account_key = "key"
        self.mock_target_client.get_container_client.return_value.get_blob_client.return_value.start_copy_from_url.side_effect = TransferError("Failed to copy")

        # Act & Assert
        ingestion_service = IngestionService(self.source_config, self.target_config)
        with self.assertRaises(TransferError):
            ingestion_service.ingest_files(self.transfer_config)


if __name__ == "__main__":
    unittest.main()