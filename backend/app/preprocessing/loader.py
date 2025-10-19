# File: app/preprocessing/loader.py
import os
import pandas as pd
from settings import RawDataFiles

class NgramFileLoader:
    def __init__(self):
        self.data_dir = RawDataFiles.RAW_DATASET_DIR
        self.file_pattern = RawDataFiles.RAW_FILE_PATTERN

    def load_files(self) -> pd.DataFrame:
        files = [f for f in os.listdir(self.data_dir) if f.endswith(self.file_pattern)]
        if not files:
            raise FileNotFoundError(f"No files matching '{self.file_pattern}' found in {self.data_dir}.")

        df_list = [self._load_file(os.path.join(self.data_dir, f)) for f in files]
        return pd.concat(df_list, ignore_index=True)

    def _load_file(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path, sep="\t", comment="#", low_memory=False)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.columns = [c.strip() for c in df.columns]  # Strip whitespace

        # Rename time columns like '2010Q1' to datetime
        ts_map = {col: pd.Period(col, freq="Q").to_timestamp() for col in df.columns if "Q" in col}
        df.rename(columns=ts_map, inplace=True)
        return df
