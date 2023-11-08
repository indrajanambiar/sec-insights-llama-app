import subprocess
import argparse 
from minio import Minio
from minio.error import S3Error
from pyshorteners import Shortener

# Minio server details
minio_endpoint = 'minio2.pixl.ai:9000'
minio_access_key = 'minio'
minio_secret_key = 'minio@123'
minio_bucket_name = 'llmapp'

parser = argparse.ArgumentParser(description="Upload a PDF to Minio and generate a short URL")
parser.add_argument("file_path", help="Path to the PDF file")
args = parser.parse_args()

# PDF file to upload
# local_pdf_file = '/home/jay/Downloads/london.pdf'
local_pdf_file = args.file_path
minio_object_name = local_pdf_file.split("/")[-1]  # The object name (key) under which the file will be stored

try:
    # Initialize the Minio client
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True 
    )

    # Check if the bucket exists, and create it if it doesn't
    if not client.bucket_exists(minio_bucket_name):
        client.make_bucket(minio_bucket_name)

    # Upload the PDF file to Minio
    client.fput_object(minio_bucket_name, minio_object_name, local_pdf_file)

    # Generate a URL for the uploaded PDF
    url = client.presigned_get_object(minio_bucket_name, minio_object_name)
    
    shortener = Shortener()
    short_url = shortener.tinyurl.short(url)

    print(f'PDF uploaded successfully. Shortened URL: {short_url}')


    # Run the upsert_document.py script with the short_url as an argument
    subprocess.run(["python", "scripts/upsert_document.py", short_url])

except S3Error as e:
    print(f"Error: {e}")