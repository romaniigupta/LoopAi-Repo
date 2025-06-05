import time, asyncio, threading
from uuid import uuid4
from storage import data_store, priority_queue, lock, PRIORITY_ORDER
from datetime import datetime

async def fetch_data(id):
    await asyncio.sleep(1) 
    return {"id": id, "data": "processed"}

def get_next_batches():
    with lock:
        
        priority_queue.sort(key=lambda x: (PRIORITY_ORDER[x['priority']], x['created_time']))
        return priority_queue[:1]  

def batch_worker():
    while True:
        if not priority_queue:
            time.sleep(1)
            continue

        batches = get_next_batches()
        if not batches:
            time.sleep(1)
            continue

        for job in batches:
            ingestion_id = job['ingestion_id']
            for batch in job['batches']:
                if batch['status'] != 'yet_to_start':
                    continue
                batch['status'] = 'triggered'
                asyncio.run(process_batch(batch, ingestion_id))
                time.sleep(5)  
            update_outer_status(ingestion_id)
            priority_queue.remove(job)

def update_outer_status(ingestion_id):
    ingestion = data_store[ingestion_id]
    batch_statuses = [b['status'] for b in ingestion['batches']]
    if all(s == 'yet_to_start' for s in batch_statuses):
        ingestion['status'] = 'yet_to_start'
    elif all(s == 'completed' for s in batch_statuses):
        ingestion['status'] = 'completed'
    else:
        ingestion['status'] = 'triggered'

async def process_batch(batch, ingestion_id):
    ids = batch['ids']
    results = await asyncio.gather(*(fetch_data(i) for i in ids))
    batch['status'] = 'completed'
    data_store[ingestion_id]['results'].append(results)

threading.Thread(target=batch_worker, daemon=True).start()