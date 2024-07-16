import logging
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from merge_csv import merge_csv_files
from google_drive import upload_csv_from_dataframe
from google_drive import upload_csv_from_dataframe, list_files 

app = FastAPI()

logging.basicConfig(level=logging.INFO)

class MergeRequest(BaseModel):
    file_ids: list
    file_name: str

class MergeResponse(BaseModel):
    file_id: str

@app.post("/merge", response_model=MergeResponse)
async def merge_files(request: MergeRequest, background_tasks: BackgroundTasks):
    try:
        file_id = await process_merge(request.file_ids, request.file_name)
        return MergeResponse(file_id=file_id)
    except Exception as e:
        logging.error(f"Error in merge_files: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def process_merge(file_ids, file_name):
    try:
        merged_df = merge_csv_files(file_ids)
        result_file_id = upload_csv_from_dataframe(file_name, merged_df)
        return result_file_id
    except Exception as e:
        logging.error(f"Error in process_merge: {e}")
        raise e

@app.get("/list-files")
async def list_drive_files():
    try:
        files = list_files()
        return {"files": files}
    except Exception as e:
        logging.error(f"Error in list_drive_files: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
