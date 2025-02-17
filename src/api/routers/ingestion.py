import os

from src.ingestion.models import BlobStorageConfig, TransferConfig
from src.ingestion.service import IngestionService


source_config = BlobStorageConfig(
    account_url="https://scrapers.blob.core.windows.net/",
    container_name="datalake",
)

target_config = BlobStorageConfig(
    account_url="https://auctionsdocuments.blob.core.windows.net/",
    container_name="poland-auctions-documents",
)

transfer_config = TransferConfig(
    source_container_name="datalake",
    source_folder_path="bronze/poland/english/komornik/document",
    target_container_name="poland-auctions-documents",
    target_folder_path="auction_notice_documents",
    filter_string="Treść obwieszczenia o e-licytacji (pdf)",
)

ingestion_service = IngestionService(source_config=source_config, target_config=target_config)
ingestion_service.ingest_files(transfer_config)
