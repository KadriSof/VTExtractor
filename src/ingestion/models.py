# Data models for ingestion
from pydantic import BaseModel, Field


class BlobStorageConfig(BaseModel):
    account_url: str = Field(..., description="Azure Blob Storage account URL.")
    container_name: str = Field(..., description="Azure Blob Storage container name.")


class TransferConfig(BaseModel):
    source_container_name: str = Field(..., description="Source container name.")
    source_folder_path: str = Field(..., description="Source folder name.")
    target_container_name: str = Field(..., description="Target container name.")
    target_folder_path: str = Field(..., description="Target folder name.")
    filter_string: str = Field(..., description="Filter string.")