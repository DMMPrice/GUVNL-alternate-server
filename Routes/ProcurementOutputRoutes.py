# backend/ProcurementOutputRoutes.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ASCENDING, DESCENDING, ReplaceOne
from bson import ObjectId
from datetime import datetime
from dotenv import load_dotenv
import os

from utils.transaction_logger import log_transaction

load_dotenv()

mongoDemandOutput_bp = Blueprint('mongoDemandOutput_bp', __name__)

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)

db = client["powercasting"]
collection = db["Demand_Output"]  # final table
approval_collection = db["Demand_Output_Approval"]  # staging table


def parse_timestamp(ts_str):
    """Try multiple formats"""
    s = ts_str.replace(" GMT", "").strip()
    for fmt in (
            '%a, %d %b %Y %H:%M:%S',
            '%Y-%m-%d %H:%M'
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {ts_str}")


# =============== Approval APIs ==================
@mongoDemandOutput_bp.route('/approvals', methods=['GET'])
def get_demand_output_approvals():
    """
    Get Demand Output Approvals (staging list)
    ---
    tags:
      - Demand Output
    """
    try:
        sort_field = request.args.get("sort", "TimeStamp")
        order = request.args.get("order", "asc").lower()
        limit = int(request.args.get("limit", 100))
        sort_order = ASCENDING if order == "asc" else DESCENDING

        cursor = approval_collection.find().sort(sort_field, sort_order).limit(limit)
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mongoDemandOutput_bp.route('/approvals/approve', methods=['POST'])
def approve_demand_output():
    """
    Approve Demand Output Data (migrate to final)
    ---
    tags:
      - Demand Output
    """
    try:
        body = request.get_json(force=True)
        ids = body.get("ids", [])
        if not ids or not isinstance(ids, list):
            return jsonify({"error": "ids must be a non-empty list"}), 400

        object_ids = [ObjectId(i) for i in ids]
        docs = list(approval_collection.find({"_id": {"$in": object_ids}}))
        if not docs:
            return jsonify({"error": "No matching documents found"}), 404

        ops = []
        for doc in docs:
            doc.pop("_id", None)
            ops.append(ReplaceOne({"TimeStamp": doc["TimeStamp"]}, doc, upsert=True))

        if ops:
            result = collection.bulk_write(ops, ordered=False)
            approval_collection.delete_many({"_id": {"$in": object_ids}})
            return jsonify({
                "message": "Demand Output approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============== PATCH approval record ================
@mongoDemandOutput_bp.route('/approvals/<_id>', methods=['PATCH'])
def update_approval_by_id(_id):
    """
    Update a pending approval record by ID
    ---
    tags:
      - Demand Output
    """
    try:
        updates = request.get_json(force=True)
        if not updates:
            return jsonify({"error": "No update data provided"}), 400

        result = approval_collection.update_one(
            {"_id": ObjectId(_id)},
            {"$set": updates}
        )

        if result.matched_count == 0:
            return jsonify({"error": "Document not found"}), 404

        return jsonify({
            "message": "Approval document updated successfully",
            "modified_count": result.modified_count
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============== DELETE approval record ================
@mongoDemandOutput_bp.route('/approvals/<_id>', methods=['DELETE'])
def delete_approval_by_id(_id):
    """
    Delete a pending approval record by ID
    ---
    tags:
      - Demand Output
    """
    try:
        result = approval_collection.delete_one({"_id": ObjectId(_id)})

        if result.deleted_count == 0:
            return jsonify({"error": "Document not found"}), 404

        return jsonify({"message": "Approval document deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
