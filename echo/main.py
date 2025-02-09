
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
    msg = request.inputs.get("input", "No había nada!")
    echo(msg)
    return {"outcome": msg}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=os.getenv("AGENT_PORT", "8000"))

