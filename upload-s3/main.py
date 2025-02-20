import base64
from io import BytesIO
import os
import boto3
import logging
from fastapi import FastAPI, File, UploadFile
from botocore.exceptions import NoCredentialsError
import requests

app = FastAPI()


logging.basicConfig(level=logging.INFO)



@app.post("/upload")
async def upload_file_to_s3(request: dict):
    try:
        input_args = request.get("inputs", {})
        file_key = input_args.get("file_key")
        file_content = input_args.get("file_content")  # Expected to be Base64
        aws_bucket = input_args.get("aws_bucket")
        aws_region = input_args.get("aws_region")

        if not file_key or not file_content or not aws_bucket or not aws_region:
            return {"error": "Missing required parameters"}

        # Decode Base64 content
        try:
            file_bytes = base64.b64decode(file_content)
        except Exception as e:
            return {"error": "Invalid Base64 file content"}

        # Construct S3 public upload URL
        file_url = f"https://{aws_bucket}.s3.{aws_region}.amazonaws.com/{file_key}"

        # Upload file via HTTP PUT
        response = requests.put(file_url, data=BytesIO(file_bytes))

        if response.status_code in [200, 204]:
            return {"message": "File uploaded successfully", "outcome": file_url}
        else:
            return {"error": f"Upload failed: {response.text}"}

    except Exception as e:
        return {"error": str(e)}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
