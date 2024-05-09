import os
import configparser
from pathlib import Path
import boto3  # Assuming AWS S3 is being used for uploads

class FolderUploader:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.access_id = self.config['spaces']['access_id']
        self.access_key = self.config['spaces']['access_secret']
        self.region_name = self.config['spaces']['region_name']
        self.endpoint_url = self.config['spaces']['endpoint_url']
        self.s3_client = boto3.client(
            's3',
            region_name = self.region_name,
            endpoint_url = self.endpoint_url, 
            aws_access_key_id=self.access_id,
            aws_secret_access_key=self.access_key
        )

    def upload_folder(self, folder_path, bucket_name, subfolder=""):
        """
        Uploads all files from the specified folder to the specified S3 bucket, possibly within a subfolder.

        Args:
        folder_path (str): The path to the folder whose files are to be uploaded.
        bucket_name (str): The S3 bucket to which files will be uploaded.
        subfolder (str): Subfolder within the S3 bucket where files will be stored.
        """
        folder_path = Path(folder_path)
        for file_path in folder_path.iterdir():
            if file_path.is_file():
                self.upload_file(file_path, bucket_name, subfolder)

    def upload_file(self, file_path, bucket_name, subfolder):
        """
        Helper method to upload a single file to an S3 bucket within a specified subfolder.

        Args:
        file_path (Path): The path to the file to upload.
        bucket_name (str): The S3 bucket to which the file will be uploaded.
        subfolder (str): Subfolder within the S3 bucket where the file will be stored.
        """
        # Ensure the subfolder path ends with a '/'
        if subfolder and not subfolder.endswith('/'):
            subfolder += '/'
        key = f"{subfolder}{file_path.name}"

        try:
            self.s3_client.upload_file(
                Filename=str(file_path),
                Bucket=bucket_name,
                Key=key
            )
            print(f"Uploaded {file_path} to {bucket_name}/{key}")
        except Exception as e:
            print(f"Failed to upload {file_path}: {str(e)}")

# Example Usage
if __name__ == "__main__":
    pass
    # uploader = FolderUploader('config.ini')
    # uploader.upload_folder('/path/to/your/folder', 'your-s3-bucket-name')
