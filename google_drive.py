import io
import logging
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive']
logging.basicConfig(level=logging.INFO)

def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)  # Disable cache discovery

def download_csv(file_id):
    service = get_drive_service()
    
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    
    except HttpError as error:
        if error.resp.status == 403 and "fileNotDownloadable" in str(error):
            export_request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, export_request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            return pd.read_csv(fh)
        else:
            raise error

def upload_csv_from_dataframe(file_name, dataframe):
    service = get_drive_service()
    file_metadata = {'name': file_name}
    
    with io.StringIO() as csv_buffer:
        dataframe.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    logging.info(f"File created with ID: {file.get('id')}")
    
    # Set permission to allow everyone to view
    permission = {
        'type': 'anyone',
        'role': 'reader'
    }
    service.permissions().create(
        fileId=file.get('id'),
        body=permission,
        fields='id',
    ).execute()
    
    return file.get('id')

def list_files():
    try:
        service = get_drive_service()
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name, owners)").execute()
        items = results.get('files', [])
        if not items:
            logging.info('No files found.')
        else:
            logging.info('Files:')
            for item in items:
                logging.info(f"{item['name']} ({item['id']})")
        return items
    except HttpError as error:
        logging.error(f'An error occurred: {error}')
        return None
