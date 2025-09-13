# backend/transaction_api.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

transactionAPI = Blueprint("transactionAPI", __name__)

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["powercasting"]
transaction_collection = db["Transaction_History"]


@transactionAPI.route("/history", methods=["GET"])
def get_transaction_history():
    """
    Get Transaction History
    ---
    tags:
      - Transactions
    parameters:
      - in: query
        name: limit
        type: integer
        required: false
        description: Number of records to return (default 10)
    responses:
      200:
        description: A list of recent transaction logs
    """
    limit = int(request.args.get("limit", 10))
    records = list(transaction_collection.find().sort("timestamp", -1).limit(limit))
    for r in records:
        r["_id"] = str(r["_id"])  # convert ObjectId to string
    return jsonify(records), 200


@transactionAPI.route("/history", methods=["DELETE"])
def delete_transaction_history():
    """
    Delete Transaction History
    ---
    tags:
      - Transactions
    parameters:
      - in: query
        name: limit
        type: integer
        required: false
        description: "Number of recent records to delete (default: delete all)"
    responses:
      200:
        description: Deletion result
    """
    try:
        limit = request.args.get("limit")

        if limit:
            limit = int(limit)
            ids_to_delete = transaction_collection.find().sort("timestamp", -1).limit(limit)
            ids = [doc["_id"] for doc in ids_to_delete]
            result = transaction_collection.delete_many({"_id": {"$in": ids}})
        else:
            result = transaction_collection.delete_many({})

        return jsonify({
            "message": "Transaction history deleted",
            "deleted_count": result.deleted_count
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
