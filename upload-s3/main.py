import os
import boto3
import logging
from fastapi import FastAPI, File, UploadFile
from botocore.exceptions import NoCredentialsError

app = FastAPI()

# pip install fastapi boto3 python-multipart


logging.basicConfig(level=logging.INFO)

# S3 Configuration (Use environment variables for security)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

@app.post("/upload")
async def upload_file_to_s3(file: UploadFile = File(...)):
    try:
        file_key = file.filename  # Use the filename as the key in S3
        logging.info(f"File key: {file_key}")

        # Upload file to S3
        s3_client.upload_fileobj(file.file, AWS_BUCKET_NAME, file_key)

        # Generate a public URL (optional)
        file_url = f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{file_key}"
        logging.info(f"File URL: {file_url}")

        return {"message": "File uploaded successfully", "outcome": file_url}
    
    except NoCredentialsError:
        return {"error": "AWS credentials not found"}

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
