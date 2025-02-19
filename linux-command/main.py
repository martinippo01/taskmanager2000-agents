import os
import subprocess
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class CommandRequest(BaseModel):
    inputs: dict

@app.post("/execute")
def execute_command(request: CommandRequest):
    try:
        command = request.inputs.get("command")
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True
        )
        return {"outcome": result.stdout, "stderr": result.stderr, "exit_code": result.returncode}
    except Exception as e:
        return {"error": str(e)}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("AGENT_IP", "0.0.0.0"), port=int(os.getenv("AGENT_PORT", "8000")))
