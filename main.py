# main.py
import os
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from merge_csv import merge_csv_files
from google_drive import upload_csv_from_dataframe, list_files
from mongodb import create_request, update_request, get_request

app = FastAPI()

class MergeRequest(BaseModel):
    file_ids: list
    file_name: str

class MergeResponse(BaseModel):
    request_id: str

@app.post("/merge", response_model=MergeResponse)
async def merge_files(request: MergeRequest, background_tasks: BackgroundTasks):
    request_id = await create_request(request.file_ids)
    background_tasks.add_task(process_merge, request_id, request.file_ids, request.file_name)
    return MergeResponse(request_id=request_id)

async def process_merge(request_id, file_ids, file_name):
    merged_df = merge_csv_files(file_ids)
    result_file_id = upload_csv_from_dataframe(file_name, merged_df)
    await update_request(request_id, result_file_id)

@app.get("/status/{request_id}")
async def check_status(request_id: str):
    request = await get_request(request_id)
    return request

@app.get("/list-files")
async def list_drive_files():
    files = list_files()
    return {"files": files}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
