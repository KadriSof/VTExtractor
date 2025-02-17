# FileHandler class
from typing import List


class FileHandler:

    @staticmethod
    def rename(folder_ref: str) -> str:
        """Rename a file according to the folder reference."""
        return f"auction_notice_pl_{folder_ref}.pdf"

    @staticmethod
    def filter_files_by_name(files: List[str], filter_string: str) -> List[str]:
        """Filter files by a substring in their names."""
        return [file for file in files if filter_string in file]