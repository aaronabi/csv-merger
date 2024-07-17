import pandas as pd
from google_drive import download_csv_in_chunks

def merge_csv_files(file_ids):
    chunk_size = 100000  # Adjust based on available memory
    dataframes = []
    
    for file_id in file_ids:
        for chunk in download_csv_in_chunks(file_id, chunk_size):
            dataframes.append(chunk)
    
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df