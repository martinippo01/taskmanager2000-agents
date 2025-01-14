
import os
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

def echo(input_string):
    print(input_string)

class EchoRequest(BaseModel):
    message: str

@app.post("/echo")
def echo_endpoint(request: EchoRequest):
    echo(request.message)
    return {"echoed_message": request.message}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("AGENT_IP", "0.0.0.0"), port=os.getenv("AGENT_PORT", "8000"))

