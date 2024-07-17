import io
import logging
import pandas as pd
import tempfile
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive']
logging.basicConfig(level=logging.INFO)

def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)

def download_csv_in_chunks(file_id, chunk_size):
    service = get_drive_service()
    
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)  # 1MB chunks
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logging.info(f"Download {int(status.progress() * 100)}%.")
        
        fh.seek(0)
        for chunk in pd.read_csv(fh, chunksize=chunk_size):
            yield chunk
    
    except HttpError as error:
        if error.resp.status == 403 and "fileNotDownloadable" in str(error):
            export_request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, export_request, chunksize=1024*1024)  # 1MB chunks
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.info(f"Download {int(status.progress() * 100)}%.")
            
            fh.seek(0)
            for chunk in pd.read_csv(fh, chunksize=chunk_size):
                yield chunk
        else:
            raise error

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
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
        dataframe.to_csv(temp_file.name, index=False)
        
        media = MediaFileUpload(temp_file.name, mimetype='text/csv', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    os.unlink(temp_file.name)  # Delete the temporary file
    
    logging.info(f"File created with ID: {file.get('id')}")
    
    try:
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
        logging.info("Permission set to allow everyone to view")

        # Share the file with your personal/work Google account
        email_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': 'automoteautomations@gmail.com'  # Replace with your actual email address
        }
        service.permissions().create(
            fileId=file.get('id'),
            body=email_permission,
            fields='id',
        ).execute()
        logging.info(f"File shared with email: automoteautomations@gmail.com")
    
    except HttpError as error:
        logging.error(f"Error setting permissions: {error}")
    
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