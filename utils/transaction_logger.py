from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri, maxPoolSize=200)
db = client["powercasting"]
transaction_collection = db["Transaction_History"]


def log_transaction(endpoint, method, request_body, request_headers,
                    response_status, response_body):
    """
    Save a transaction log into MongoDB
    """
    try:
        user_email = request_headers.get("X-User-Email") or None
        log_entry = {
            "author": user_email,
            "endpoint": endpoint,
            "method": method,
            "request_body": request_body,
            "response_status": response_status,
            "response_body": response_body,
            "timestamp": datetime.utcnow()
        }
        transaction_collection.insert_one(log_entry)
    except Exception as e:
        print(f"[Transaction Logger Error] {e}")
