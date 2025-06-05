import asyncio
from fastapi import FastAPI
from models import IngestRequest
from storage import data_store, priority_queue
from uuid import uuid4
from datetime import datetime
from fastapi.responses import JSONResponse

app = FastAPI()


@app.on_event("startup")
async def start_processing():
    asyncio.create_task(process_queue())

async def process_queue():
    while True:
        if priority_queue:
            priority_queue.sort(key=lambda x: x["priority"])
            task = priority_queue.pop(0)
            ingestion_id = task["ingestion_id"]
            data_store[ingestion_id]["status"] = "in_progress"

            for batch in task["batches"]:
                batch["status"] = "in_progress"
                await asyncio.sleep(2) 
                batch["status"] = "completed"
                data_store[ingestion_id]["results"].append({
                    "batch_id": batch["batch_id"],
                    "output": f"Processed {batch['ids']}"
                })

            data_store[ingestion_id]["status"] = "completed"
        else:
            await asyncio.sleep(1)


@app.post("/ingest")
def ingest(request: IngestRequest):
    ingestion_id = str(uuid4())
    ids = request.ids
    batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
    batch_objs = [{"batch_id": str(uuid4()), "ids": b, "status": "yet_to_start"} for b in batches]

    data_store[ingestion_id] = {
        "status": "yet_to_start",
        "batches": batch_objs,
        "results": []
    }

    priority_queue.append({
        "ingestion_id": ingestion_id,
        "priority": request.priority,
        "created_time": datetime.now(),
        "batches": batch_objs
    })

    return JSONResponse({"ingestion_id": ingestion_id})


@app.get("/status/{ingestion_id}")
def status(ingestion_id: str):
    if ingestion_id not in data_store:
        return JSONResponse({"error": "Not found"}, status_code=404)

    record = data_store[ingestion_id]
    return {
        "ingestion_id": ingestion_id,
        "status": record['status'],
        "batches": record['batches']
    }


@app.get("/results/{ingestion_id}")
def get_results(ingestion_id: str):
    if ingestion_id not in data_store:
        return JSONResponse({"error": "Not found"}, status_code=404)

    return {
        "ingestion_id": ingestion_id,
        "results": data_store[ingestion_id]["results"]
    }