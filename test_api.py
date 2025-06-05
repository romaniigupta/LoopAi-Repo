import time
import requests

base_url = "http://localhost:8000"

def test_ingestion():
    r1 = requests.post(f"{base_url}/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    id1 = r1.json()["ingestion_id"]

    time.sleep(4)
    r2 = requests.post(f"{base_url}/ingest", json={"ids": [6, 7, 8, 9], "priority": "HIGH"})
    id2 = r2.json()["ingestion_id"]

    print("Submitted both ingestion requests")
    time.sleep(1)

    for _ in range(6):
        print("Status 1:", requests.get(f"{base_url}/status/{id1}").json())
        print("Status 2:", requests.get(f"{base_url}/status/{id2}").json())
        time.sleep(5)

test_ingestion()