import os
import io
import boto3
import logging
import mimetypes
import base64
from fastapi import FastAPI, HTTPException
import requests
from botocore.exceptions import NoCredentialsError, ClientError

app = FastAPI()

# S3 Configuration
#AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
#AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
#AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
#AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize S3 client


logging.basicConfig(level=logging.INFO)

@app.post("/download")
async def download_file_from_s3(request: dict):
    try:
        input_args = request.get("inputs", {})

        aws_bucket = input_args.get("aws_bucket")
        file_name = input_args.get("file_name")

        if not aws_bucket or not file_name:
            raise HTTPException(status_code=400, detail="Missing 'aws_bucket' or 'file_name'")

        # Construct the public URL
        file_url = f"https://{aws_bucket}.s3.amazonaws.com/{file_name}"

        logging.info(f"Fetching public file from {file_url}")

        # Fetch the file from S3 using HTTP GET
        response = requests.get(file_url)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="File not found or access denied")

        file_content = response.content

        # Encode file content as Base64
        base64_encoded = base64.b64encode(file_content).decode("utf-8")

        logging.info(f"Successfully fetched and encoded '{file_name}'.")

        # Return the Base64-encoded file in JSON
        return {
            "outcome": base64_encoded,
            "file_name": file_name,
            "mime_type": mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        }

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/ping")
def ping():
    return {"ping": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
