import os
import io
import boto3
import logging
import mimetypes
import base64
from fastapi import FastAPI, HTTPException, Request
from botocore.exceptions import NoCredentialsError, ClientError

app = FastAPI()

# S3 Configuration
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

logging.basicConfig(level=logging.INFO)

@app.post("/download")
async def download_file_from_s3(request: Request):
    try:
        # Parse JSON request body
        body = await request.json()
        input_args = body.get("inputs", {})

        # Extract file name
        file_name = input_args.get("file_name")
        if not file_name:
            raise HTTPException(status_code=400, detail="Missing 'file_name' in inputArgs")

        logging.info(f"Fetching file '{file_name}' from S3 bucket '{AWS_BUCKET_NAME}'.")

        # Fetch the file from S3
        file_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=file_name)
        file_content = file_obj["Body"].read()

        # Encode file content as Base64
        base64_encoded = base64.b64encode(file_content).decode("utf-8")

        logging.info(f"Successfully fetched and encoded '{file_name}'.")

        # Return the Base64-encoded file in JSON
        return {
            "outcome": base64_encoded,
            "file_name": file_name,
            "mime_type": mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        }

    except NoCredentialsError:
        logging.error("AWS credentials not found.")
        raise HTTPException(status_code=500, detail="AWS credentials not found")

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.error(f"File '{file_name}' not found in S3.")
            raise HTTPException(status_code=404, detail="File not found in S3")
        else:
            logging.error(f"AWS ClientError: {str(e)}")
            raise HTTPException(status_code=500, detail="Error retrieving file from S3")

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
