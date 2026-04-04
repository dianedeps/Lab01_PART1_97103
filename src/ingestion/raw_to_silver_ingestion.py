from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


class RawToSilverIngestion:
    """
    Orchestrates the ingestion of CSV files from the raw layer to the silver layer.
    """

    def __init__(self, raw_dir: str, silver_dir: str, metadata_dir: str, ) -> None:
        self.raw_dir = Path(raw_dir)
        self.silver_dir = Path(silver_dir)
        self.metadata_dir = Path(metadata_dir)

        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def list_raw_files(self) -> List[Path]:
        """
        List all CSV files recursively in the raw directory.
        """
        return sorted(self.raw_dir.rglob("*.csv"))

    def read_csv_file(self, file_path: Path) -> pd.DataFrame:
        """
        Read a CSV file forcing all columns to string.
        """
        return pd.read_csv(file_path, dtype=str, sep=";", encoding="utf-8-sig")

    def extract_loaded_schema(self, dataframe: pd.DataFrame) -> Dict[str, str]:
        """
        Return the logical schema used in this ingestion.
        """
        return {column: "string" for column in dataframe.columns}

    def build_metadata(self, file_path: Path, dataframe: pd.DataFrame) -> Dict:
        """
        Build ingestion metadata for the processed file.
        """
        relative_file_path = file_path.relative_to(self.raw_dir)

        return {
            "source_file_name": file_path.name,
            "source_relative_path": str(relative_file_path),
            "source_parent_folder": file_path.parent.name,
            "row_count": int(len(dataframe)),
            "column_count": int(len(dataframe.columns)),
            "columns": list(dataframe.columns),
            "loaded_schema": self.extract_loaded_schema(dataframe),
            "read_strategy": "All columns loaded as string from CSV",
        }

    def save_schema_metadata(self, source_file_name: str, metadata: Dict) -> None:
        """
        Save ingestion metadata as a JSON file.
        """
        output_file = self.metadata_dir / f"{Path(source_file_name).stem}_metadata.json"

        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(metadata, file, ensure_ascii=False, indent=4)

    def save_as_parquet(self, dataframe: pd.DataFrame, source_file_name: str) -> None:
        """
        Save the dataframe as Parquet in the silver directory.
        """
        output_file = self.silver_dir / f"{Path(source_file_name).stem}.parquet"
        dataframe.to_parquet(output_file, index=False)

    def process_file(self, file_path: Path) -> None:
        """
        Process a single raw CSV file into the silver layer.
        """
        print(f"Processing file: {file_path}")

        dataframe = self.read_csv_file(file_path)
        metadata = self.build_metadata(file_path, dataframe)

        self.save_as_parquet(dataframe, file_path.name)
        self.save_schema_metadata(file_path.name, metadata)

        print(f"Finished processing: {file_path.name}")

    def run(self) -> None:
        """
        Execute the ingestion process for all CSV files in the raw directory.
        """
        raw_files = self.list_raw_files()

        if not raw_files:
            print(f"No CSV files found in: {self.raw_dir}")
            return

        print(f"Found {len(raw_files)} CSV file(s) in {self.raw_dir}")

        for file_path in raw_files:
            self.process_file(file_path)