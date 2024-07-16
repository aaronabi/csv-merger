# merge_csv.py
import pandas as pd
from google_drive import download_csv

def merge_csv_files(file_ids):
    dataframes = [download_csv(file_id) for file_id in file_ids]
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df
