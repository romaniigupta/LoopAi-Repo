from collections import defaultdict
from uuid import uuid4
from datetime import datetime
from threading import Lock

data_store = {}
priority_queue = []
lock = Lock()

PRIORITY_ORDER = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}