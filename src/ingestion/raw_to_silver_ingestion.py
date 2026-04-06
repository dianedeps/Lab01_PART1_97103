from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, List

import pandas as pd


class RawToSilverIngestion:
    """
    Orchestrates the ingestion of CSV files from the raw layer to the silver layer.
    """

    def __init__(self, raw_dir: str, silver_dir: str, metadata_dir: str) -> None:
        self.raw_dir = Path(raw_dir)
        self.silver_dir = Path(silver_dir)
        self.metadata_dir = Path(metadata_dir)

        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def list_raw_files(self) -> List[Path]:
        return sorted(self.raw_dir.rglob("*.csv"))

    def read_csv_file(self, file_path: Path) -> pd.DataFrame:
        return pd.read_csv(file_path, dtype=str, sep=";", encoding="utf-8-sig")

    def extract_loaded_schema(self, dataframe: pd.DataFrame) -> Dict[str, str]:
        return {column: "string" for column in dataframe.columns}

    def extract_domain_and_date(self, file_name: str) -> tuple[str, str]:
        """
        Expected pattern:
        <domain_name>_YYYYMMDD.csv
        Example:
        acidentes_dados_abertos_20230613.csv
        """
        stem = Path(file_name).stem
        parts = stem.split("_")

        reference_date = parts[-1]
        domain_name = "_".join(parts[:-1])

        return domain_name, reference_date

    def normalize_extended_flag(self, series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.strip()
            .str.upper()
            .map({
                "SIM": True,
                "NAO": False,
                "NÃO": False,
                "NAO INFORMADO": None,
                "DESCONHECIDO": "",
                "NAO APLICAVEL": "NA",
            })
        )

    def apply_transformations(self, df: pd.DataFrame, domain_name: str) -> pd.DataFrame:
        df = df.copy()

        if domain_name == "acidentes_dados_abertos":
            df["ind_guardrail"] = self.normalize_extended_flag(df["ind_guardrail"])
            df["ind_cantcentral"] = self.normalize_extended_flag(df["ind_cantcentral"])
            df["ind_acostamento"] = self.normalize_extended_flag(df["ind_acostamento"])

        if domain_name == "localidade":
            df["regiao_metropolitana"] = self.normalize_extended_flag(df["regiao_metropolitana"])

        if domain_name == "vitimas":
            df["ind_motorista"] = self.normalize_extended_flag(df["ind_motorista"])
            df["susp_alcool"] = self.normalize_extended_flag(df["susp_alcool"])

        if domain_name == "tipo_veiculo":
            df["ind_veic_estrangeiro"] = self.normalize_extended_flag(df["ind_veic_estrangeiro"])

        if domain_name == "vitimas":
            df["qtde_envolvidos"] = pd.to_numeric(df["qtde_envolvidos"], errors="coerce")
            df["qtde_feridosilesos"] = pd.to_numeric(df["qtde_feridosilesos"], errors="coerce")
            df["qtde_obitos"] = pd.to_numeric(df["qtde_obitos"], errors="coerce")

            df["flag_vitima"] = (df["qtde_envolvidos"] > 0)
            df["flag_ferido"] = (df["qtde_feridosilesos"] > 0)
            df["flag_obito"] = (df["qtde_obitos"] > 0)

        return df

    def add_control_columns(self, dataframe: pd.DataFrame, file_path: Path) -> pd.DataFrame:
        domain_name, reference_date = self.extract_domain_and_date(file_path.name)

        dataframe = dataframe.copy()

        dataframe["source_file_name"] = file_path.name
        dataframe["source_relative_path"] = str(file_path.relative_to(self.raw_dir))
        dataframe["source_parent_folder"] = file_path.parent.name
        dataframe["domain_name"] = domain_name
        dataframe["reference_date"] = reference_date

        return dataframe

    def group_files_by_domain(self, files: List[Path]) -> Dict[str, List[Path]]:
        grouped_files: Dict[str, List[Path]] = {}

        for file_path in files:
            domain_name, _ = self.extract_domain_and_date(file_path.name)

            if domain_name not in grouped_files:
                grouped_files[domain_name] = []

            grouped_files[domain_name].append(file_path)

        return grouped_files

    def build_metadata(self, file_path: Path, dataframe: pd.DataFrame) -> Dict:
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
        output_file = self.metadata_dir / f"{Path(source_file_name).stem}_metadata.json"

        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(metadata, file, ensure_ascii=False, indent=4)

    def save_as_parquet(self, dataframe: pd.DataFrame, source_file_name: str) -> None:
        output_file = self.silver_dir / f"{Path(source_file_name).stem}.parquet"
        dataframe.to_parquet(output_file, index=False)

    def reset_domain_output(self, domain_name: str) -> Path:
        """
        Creates/cleans the output folder for the stacked domain dataset.
        """
        domain_output_dir = self.silver_dir / domain_name

        if domain_output_dir.exists():
            shutil.rmtree(domain_output_dir)

        domain_output_dir.mkdir(parents=True, exist_ok=True)
        return domain_output_dir

    def stack_domain_files(self, domain_name: str, files: List[Path]) -> None:
        """
        Stack files from the same domain without concatenating everything in memory.
        Saves one parquet per source file inside a domain folder.

        Final structure example:
        silver/
            acidentes_dados_abertos/
                part_0001.parquet
                part_0002.parquet
        """
        print(f"Stacking domain: {domain_name} ({len(files)} file(s))")

        domain_output_dir = self.reset_domain_output(domain_name)

        for idx, file_path in enumerate(sorted(files), start=1):
            dataframe = self.read_csv_file(file_path)
            dataframe = self.add_control_columns(dataframe, file_path)

            dataframe = self.apply_transformations(dataframe, domain_name)

            output_file = domain_output_dir / f"part_{idx:04d}.parquet"
            dataframe.to_parquet(output_file, index=False)

            print(f"  Saved: {output_file.name}")

    def process_file(self, file_path: Path) -> None:
        print(f"Processing file: {file_path}")

        dataframe = self.read_csv_file(file_path)
        domain_name, _ = self.extract_domain_and_date(file_path.name)

        dataframe = self.add_control_columns(dataframe, file_path)
        dataframe = self.apply_transformations(dataframe, domain_name)

        metadata = self.build_metadata(file_path, dataframe)

        self.save_as_parquet(dataframe, file_path.name)
        self.save_schema_metadata(file_path.name, metadata)

        print(f"Finished processing: {file_path.name}")

    def run(self) -> None:
        raw_files = self.list_raw_files()

        if not raw_files:
            print(f"No CSV files found in: {self.raw_dir}")
            return

        print(f"Found {len(raw_files)} CSV file(s) in {self.raw_dir}")

        grouped_files = self.group_files_by_domain(raw_files)

        # Step 1: create stacked domain datasets
        for domain_name, files in grouped_files.items():
            self.stack_domain_files(domain_name, files)

        # Step 2: keep original per-file processing
        for file_path in raw_files:
            self.process_file(file_path)