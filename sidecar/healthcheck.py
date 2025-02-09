import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()

# TODO: Podría chequear el kakfa

@app.get("/ping")
def health_check():
    return {"status": "healthy"}


def start_fastapi():
    portForHealthcheck = os.getenv("HEALTHCHECK_PORT", 8500)
    uvicorn.run(app, host="0.0.0.0", port=portForHealthcheck)
