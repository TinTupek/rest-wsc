import os
import asyncpg
import csv
import json
import logging
import asyncio
from typing import List, Tuple, Any
from dataclasses import dataclass
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessedData:
    columns: List[str]
    rows: List[List[Any]]
    file_type: str
    batch_id: str

class WeatherDataFetcher:
    def __init__(self, database_url: str, service_account_file: str, drive_folder_id: str):
        self.database_url = database_url
        self.service_account_file = service_account_file
        self.drive_folder_id = drive_folder_id
        self.supported_file_types = {'csv', 'json'}
        self.drive_service = self.authenticate_drive()

    def authenticate_drive(self):
        """Authenticate Google Drive using a service account."""
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials)

    async def fetch_data(self) -> Tuple[List[str], List[Tuple]]:
        """Fetch weather data from the database."""
        try:
            conn = await asyncpg.connect(self.database_url)
            rows = await conn.fetch("SELECT * FROM public.pull_weather_data()")
            
            if not rows:
                logger.info("No data returned from the pull_weather_data function.")
                return [], []
            
            columns = list(rows[0].keys())
            rows = [tuple(row.values()) for row in rows]
            logger.info(f"Fetched {len(rows)} rows from the pull_weather_data function.")
            return columns, rows

        except Exception as e:
            logger.error(f"Error fetching data from database: {e}")
            return [], []
        
        finally:
            if 'conn' in locals():
                await conn.close()
                logger.info("Database connection closed.")

    def process_data(self, columns: List[str], rows: List[Tuple]) -> ProcessedData:
        """Process the fetched data and extract metadata."""
        file_type = "csv"
        processed_columns = columns.copy()
        processed_rows = [list(row) for row in rows]

        if "file_type" in columns:
            idx = columns.index("file_type")
            file_type = rows[0][idx]
            processed_columns.pop(idx)
            processed_rows = [row[:idx] + row[idx+1:] for row in processed_rows]

        if "batch_id" in processed_columns:
            idx = processed_columns.index("batch_id")
            batch_id = rows[0][idx]
            processed_columns.pop(idx)
            processed_rows = [row[:idx] + row[idx+1:] for row in processed_rows]
        else:
            raise ValueError("batch_id column is missing from the query results")

        return ProcessedData(processed_columns, processed_rows, file_type, batch_id)

    def save_data(self, data: ProcessedData) -> str:
        """Save data to file in the specified format and return the filename."""
        filename = f"{data.batch_id}.{data.file_type}"
        
        if data.file_type not in self.supported_file_types:
            logger.error(f"Unsupported file type: {data.file_type}")
            return None

        try:
            if data.file_type == 'csv':
                self._save_csv(data.columns, data.rows, filename)
            else:
                self._save_json(data.columns, data.rows, filename)
            logger.info(f"Data saved to {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving {data.file_type} file: {e}")
            return None

    def _save_csv(self, columns: List[str], rows: List[List], filename: str) -> None:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(rows)

    def _save_json(self, columns: List[str], rows: List[List], filename: str) -> None:
        data = [dict(zip(columns, row)) for row in rows]
        with open(filename, mode='w') as file:
            json.dump(data, file, indent=4)

    def upload_to_drive_and_cleanup(self, filename: str) -> None:
        """Upload a file to Google Drive and delete it locally."""
        try:
            file_metadata = {'name': filename, 'parents': [self.drive_folder_id]}
            media = MediaFileUpload(filename, resumable=True)
            uploaded_file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logger.info(f"Uploaded {filename} to Google Drive with ID: {uploaded_file.get('id')}")

            # Delete the file locally after upload
            os.remove(filename)
            logger.info(f"Deleted local file {filename} after uploading.")
        except Exception as e:
            logger.error(f"Failed to upload {filename} to Google Drive: {e}")

async def main():
    interval = int(os.getenv("POLL_INTERVAL", 15))
    database_url = os.getenv("DATABASE_URL")
    service_account_file = '/app/credentials/service-account.json'
    drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        return

    fetcher = WeatherDataFetcher(database_url, service_account_file, drive_folder_id)
    
    while True:
        columns, rows = await fetcher.fetch_data()
        
        if columns and rows:
            processed_data = fetcher.process_data(columns, rows)
            filename = fetcher.save_data(processed_data)
            if filename:
                fetcher.upload_to_drive_and_cleanup(filename)
        
        logger.info(f"Waiting {interval} seconds before next poll")
        await asyncio.sleep(interval)

if __name__ == "__main__":
    asyncio.run(main())
