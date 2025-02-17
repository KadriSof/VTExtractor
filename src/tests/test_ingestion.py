import sys
import os
import unittest

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from unittest.mock import MagicMock, patch
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ClientAuthenticationError
from tenacity import RetryError
from src.ingestion.models import BlobStorageConfig, TransferConfig
from src.ingestion.service import IngestionService
from src.common.exceptions import TransferError

load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
default_credential = DefaultAzureCredential()


class TestIngestionService(unittest.TestCase):
    def setUp(self):
        # Mock Configuration
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

        self.ingestion_service = IngestionService(self.source_config, self.target_config)

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_success(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]

        # Mock list_blobs to return a list of blobs
        mock_blob1 = MagicMock()
        mock_blob1.name = "file1.pdf"
        mock_blob2 = MagicMock()
        mock_blob2.name = "file2.pdf"
        self.mock_source_client.get_container_client.return_value.list_blobs.return_value = [mock_blob1, mock_blob2]

        # Mock generate_user_delegation_sas to return a dummy SAS token
        with patch('src.ingestion.blob_manager.BlobServiceManager.generate_user_delegation_sas') as mock_generate_sas:
            mock_generate_sas.return_value = "valid_sas_token"

            # Mock successful copy operation
            mock_copy_operation = MagicMock()
            mock_copy_operation.status.return_value = {"copy_status": "success"}
            (self.mock_target_client.get_container_client
             .return_value
             .get_blob_client
             .return_value
             .start_copy_from_url.return_value) = mock_copy_operation

            # Act
            ingestion_service = IngestionService(self.source_config, self.target_config)
            ingestion_service.ingest_files(self.transfer_config)

            # Assert
            # Add assertions to verify the expected behavior, e.g., check if start_copy_from_url was called
            self.mock_target_client.get_container_client.return_value.get_blob_client.return_value.start_copy_from_url.assert_called()

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_blob_storage_error(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]

        # Mock list_blobs to raise a ClientAuthenticationError
        self.mock_source_client.get_container_client.return_value.list_blobs.side_effect = ClientAuthenticationError("Authentication failed")

        # Act & Assert
        ingestion_service = IngestionService(self.source_config, self.target_config)
        with self.assertRaises(RetryError) as context:
            ingestion_service.ingest_files(self.transfer_config)

        # Check if the underlying exception is ClientAuthenticationError
        self.assertIsInstance(context.exception.__cause__, TransferError)
        self.assertIsInstance(context.exception.__cause__, ClientAuthenticationError)

    @patch("azure.storage.blob.BlobServiceClient")
    def test_ingest_files_transfer_error(self, mock_blob_service_client):
        # Arrange
        mock_blob_service_client.side_effect = [self.mock_source_client, self.mock_target_client]
        self.mock_source_client.get_container_client.return_value.list_blobs.return_value = [
            MagicMock(name="file1.pdf"),
        ]

        # Mock start_copy_from_url to raise a TransferError
        (self.mock_target_client.get_container_client
         .return_value
         .get_blob_client
         .return_value
         .start_copy_from_url
         .side_effect) = TransferError("Failed to copy")

        # Act & Assert
        ingestion_service = IngestionService(self.source_config, self.target_config)
        with self.assertRaises(RetryError) as context:
            ingestion_service.ingest_files(self.transfer_config)

        # Check if the underlying exception is TransferError
        self.assertIsInstance(context.exception.__cause__, TransferError)


if __name__ == "__main__":
    unittest.main()