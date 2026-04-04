
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import matplotlib.pyplot as plt
import pandas as pd


@dataclass
class DatasetProfileResult:
    """Container for profiling outputs of a single dataset."""

    name: str
    dataframe: pd.DataFrame
    nulls: pd.DataFrame
    column_types: pd.DataFrame
    descriptive_stats: pd.DataFrame
    duplicates_count: int
    row_count: int
    column_count: int


class RawDataProfiler:
    """
    Simple profiler for raw CSV datasets.

    Recommended use:
    1. Point dataset_paths to the raw CSV files.
    2. Run profile_dataset() or profile_all().
    3. Review nulls, dtypes, descriptive stats and plots.
    """

    def __init__(
        self,
        dataset_paths: Dict[str, str],
        output_dir: Optional[str] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        sample_rows: Optional[int] = None,
    ) -> None:
        self.dataset_paths = dataset_paths
        self.output_dir = Path(output_dir) if output_dir else None
        self.sep = sep
        self.encoding = encoding
        self.sample_rows = sample_rows

        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_csv(self, file_path: str) -> pd.DataFrame:
        """Load CSV as strings to preserve raw layer values."""

        return pd.read_csv(
            file_path,
            sep=self.sep or ";",
            dtype=str,
            keep_default_na=True,
            na_values=["", " ", "NULL", "null", "None"],
            nrows=self.sample_rows,
            encoding=self.encoding or "utf-8",
            engine="python",
        )

    def _build_nulls_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        null_count = df.isna().sum()
        null_pct = (null_count / len(df) * 100).round(2) if len(df) > 0 else 0

        result = pd.DataFrame(
            {
                "column_name": df.columns,
                "null_count": null_count.values,
                "null_pct": null_pct.values if hasattr(null_pct, "values") else [0] * len(df.columns),
                "non_null_count": df.notna().sum().values,
                "distinct_count": df.nunique(dropna=True).values,
            }
        ).sort_values(["null_count", "column_name"], ascending=[False, True])

        return result.reset_index(drop=True)

    def _build_column_types_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        inferred_types = []
        for column in df.columns:
            numeric_ratio = pd.to_numeric(df[column], errors="coerce").notna().mean()
            datetime_ratio = pd.to_datetime(df[column], errors="coerce").notna().mean()

            if numeric_ratio >= 0.9:
                inferred_type = "probably_numeric"
            elif datetime_ratio >= 0.9:
                inferred_type = "probably_datetime"
            else:
                inferred_type = "string_or_categorical"

            inferred_types.append(inferred_type)

        return pd.DataFrame(
            {
                "column_name": df.columns,
                "pandas_dtype": [str(df[column].dtype) for column in df.columns],
                "inferred_type": inferred_types,
            }
        )

    def _build_descriptive_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_df = df.apply(pd.to_numeric, errors="coerce")
        stats = numeric_df.describe().T if not numeric_df.dropna(axis=1, how="all").empty else pd.DataFrame()

        if stats.empty:
            return pd.DataFrame(columns=["column_name", "count", "mean", "std", "min", "25%", "50%", "75%", "max"])

        stats = stats.reset_index().rename(columns={"index": "column_name"})
        return stats

    def profile_dataset(self, dataset_name: str) -> DatasetProfileResult:
        """Profile one dataset."""
        file_path = self.dataset_paths[dataset_name]
        df = self.load_csv(file_path)

        result = DatasetProfileResult(
            name=dataset_name,
            dataframe=df,
            nulls=self._build_nulls_summary(df),
            column_types=self._build_column_types_summary(df),
            descriptive_stats=self._build_descriptive_stats(df),
            duplicates_count=int(df.duplicated().sum()),
            row_count=len(df),
            column_count=len(df.columns),
        )

        return result

    def profile_all(self) -> Dict[str, DatasetProfileResult]:
        """Profile all configured datasets."""
        return {dataset_name: self.profile_dataset(dataset_name) for dataset_name in self.dataset_paths}

    def create_overview_summary(self, results: Dict[str, DatasetProfileResult]) -> pd.DataFrame:
        """Create one consolidated summary for all datasets."""
        rows = []
        for dataset_name, result in results.items():
            rows.append(
                {
                    "dataset_name": dataset_name,
                    "rows": result.row_count,
                    "columns": result.column_count,
                    "duplicate_rows": result.duplicates_count,
                    "columns_with_nulls": int((result.nulls["null_count"] > 0).sum()),
                    "max_null_pct": float(result.nulls["null_pct"].max()) if not result.nulls.empty else 0.0,
                }
            )

        return pd.DataFrame(rows).sort_values("dataset_name").reset_index(drop=True)

    def plot_nulls(self, result: DatasetProfileResult, top_n: int = 15) -> None:
        """Plot top columns with most nulls."""
        plot_df = result.nulls[result.nulls["null_count"] > 0].head(top_n)

        if plot_df.empty:
            print(f"[{result.name}] No nulls found.")
            return

        plt.figure(figsize=(12, 6))
        plt.bar(plot_df["column_name"], plot_df["null_count"])
        plt.xticks(rotation=75, ha="right")
        plt.title(f"{result.name} - Top columns by null count")
        plt.ylabel("Null count")
        plt.xlabel("Column")
        plt.tight_layout()
        plt.show()

    def plot_distinct_counts(self, result: DatasetProfileResult, top_n: int = 15) -> None:
        """Plot columns with the highest cardinality."""
        plot_df = result.nulls.sort_values("distinct_count", ascending=False).head(top_n)

        plt.figure(figsize=(12, 6))
        plt.bar(plot_df["column_name"], plot_df["distinct_count"])
        plt.xticks(rotation=75, ha="right")
        plt.title(f"{result.name} - Top columns by distinct values")
        plt.ylabel("Distinct count")
        plt.xlabel("Column")
        plt.tight_layout()
        plt.show()

    def plot_top_categories(self, result: DatasetProfileResult, column_name: str, top_n: int = 10) -> None:
        """Plot the most frequent values for a categorical column."""
        if column_name not in result.dataframe.columns:
            print(f"Column '{column_name}' not found in dataset '{result.name}'.")
            return

        plot_df = (
            result.dataframe[column_name]
            .fillna("<<NULL>>")
            .value_counts(dropna=False)
            .head(top_n)
            .reset_index()
        )
        plot_df.columns = ["value", "count"]

        plt.figure(figsize=(12, 6))
        plt.bar(plot_df["value"].astype(str), plot_df["count"])
        plt.xticks(rotation=75, ha="right")
        plt.title(f"{result.name} - Top {top_n} values for {column_name}")
        plt.ylabel("Count")
        plt.xlabel(column_name)
        plt.tight_layout()
        plt.show()

    def plot_numeric_distribution(self, result: DatasetProfileResult, column_name: str, bins: int = 30) -> None:
        """Plot histogram for a numeric-like column."""
        if column_name not in result.dataframe.columns:
            print(f"Column '{column_name}' not found in dataset '{result.name}'.")
            return

        numeric_series = pd.to_numeric(result.dataframe[column_name], errors="coerce").dropna()

        if numeric_series.empty:
            print(f"Column '{column_name}' in dataset '{result.name}' is not numeric enough for a histogram.")
            return

        plt.figure(figsize=(12, 6))
        plt.hist(numeric_series, bins=bins)
        plt.title(f"{result.name} - Numeric distribution for {column_name}")
        plt.ylabel("Frequency")
        plt.xlabel(column_name)
        plt.tight_layout()
        plt.show()

    def export_results(self, results: Dict[str, DatasetProfileResult]) -> None:
        """Export summary tables as CSV files."""
        if not self.output_dir:
            print("output_dir not provided. Skipping export.")
            return

        overview = self.create_overview_summary(results)
        overview.to_csv(self.output_dir / "overview_summary.csv", index=False)

        for dataset_name, result in results.items():
            safe_name = dataset_name.lower().replace(" ", "_")
            result.nulls.to_csv(self.output_dir / f"{safe_name}_nulls.csv", index=False)
            result.column_types.to_csv(self.output_dir / f"{safe_name}_column_types.csv", index=False)
            result.descriptive_stats.to_csv(self.output_dir / f"{safe_name}_descriptive_stats.csv", index=False)

        print(f"Profiling files exported to: {self.output_dir}")
