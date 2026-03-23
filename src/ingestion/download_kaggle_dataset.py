import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
import kagglehub


class KaggleDatasetDownloader:
    """
    Handles Kaggle dataset download into a target local directory.
    """

    def __init__(self, dataset_slug: str, output_dir: str) -> None:
        self.dataset_slug = dataset_slug
        self.output_dir = Path(output_dir)

    def load_environment(self) -> None:
        """
        Load local environment variables and set Kaggle token.
        """
        load_dotenv()

        kaggle_api_token = os.getenv("KAGGLE_API_TOKEN")

        if not kaggle_api_token:
            raise ValueError("KAGGLE_API_TOKEN was not found in the .env file.")

        os.environ["KAGGLE_API_TOKEN"] = kaggle_api_token

    def prepare_output_directory(self) -> None:
        """
        Create output directory if it does not exist.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download_dataset(self) -> None:
        """
        Download dataset from KaggleHub cache and copy files to output directory.
        """
        dataset_cache_path = Path(kagglehub.dataset_download(self.dataset_slug))

        for item in dataset_cache_path.iterdir():
            target_path = self.output_dir / item.name

            if item.is_file():
                shutil.copy2(item, target_path)
            elif item.is_dir():
                if target_path.exists():
                    shutil.rmtree(target_path)
                shutil.copytree(item, target_path)

        print(f"Dataset downloaded successfully to: {self.output_dir.resolve()}")

    def run(self) -> None:
        """
        Main execution flow.
        """
        self.load_environment()
        self.prepare_output_directory()
        self.download_dataset()