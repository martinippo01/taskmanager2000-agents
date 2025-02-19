
import os
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

def echo(input_string):
    print(input_string)

class EchoRequest(BaseModel):
    inputs: dict

@app.post("/echo")
def echo_endpoint(request: EchoRequest):
    msg = request.inputs.get("input")
    if not msg:
        msg = "Nothing to echo"
    echo(msg)
    return {"outcome": msg}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("AGENT_IP", "0.0.0.0"), port=int(os.getenv("AGENT_PORT", "8000")))

