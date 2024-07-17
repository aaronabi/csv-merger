from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import asyncio
from concurrent.futures import ThreadPoolExecutor
from merge_csv import merge_csv_files
from google_drive import upload_csv_from_dataframe
import logging

app = FastAPI()

class MergeRequest(BaseModel):
    file_ids: List[str]
    file_name: str

@app.post("/merge")
async def merge_csv(request: MergeRequest):
    if len(request.file_ids) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 files can be merged at once")
    
    try:
        result_file_id = await process_merge(request.file_ids, request.file_name)
        return {"result_file_id": result_file_id}
    except Exception as e:
        logging.error(f"Error during merge: {e}")
        raise HTTPException(status_code=500, detail="An error occurred during the merge process")

async def process_merge(file_ids, file_name):
    try:
        executor = ThreadPoolExecutor(max_workers=5)  # Adjust based on your server's capabilities
        loop = asyncio.get_event_loop()
        merged_df = await loop.run_in_executor(executor, merge_csv_files, file_ids)
        result_file_id = await loop.run_in_executor(executor, upload_csv_from_dataframe, file_name, merged_df)
        return result_file_id
    except Exception as e:
        logging.error(f"Error in process_merge: {e}")
        raise e

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)