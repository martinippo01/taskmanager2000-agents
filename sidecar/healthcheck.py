import logging
import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/ping")
def health_check():
    return {"status": "healthy"}


class NoPingFilter(logging.Filter):
    def filter(self, record):
        return "/ping" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoPingFilter())

def start_fastapi():
    portForHealthcheck = int(os.getenv("HEALTHCHECK_PORT", '8500'))
    uvicorn.run(app, host="0.0.0.0", port=portForHealthcheck)
