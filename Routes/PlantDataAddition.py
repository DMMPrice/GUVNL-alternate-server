# backend/plant_api.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ReplaceOne, ASCENDING, DESCENDING
from bson import ObjectId
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

plantAPI = Blueprint("plantAPI", __name__)

# ── MongoDB setup ───────────────────────────────────────────────────
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri, maxPoolSize=200)
db = client["powercasting"]

# Staging + Final collections
collection = db["mustrunplantconsumption_approval"]
final_collection = db["mustrunplantconsumption"]

# Ensure composite unique index on staging
try:
    collection.create_index([("TimeStamp", ASCENDING), ("Plant_Name", ASCENDING)], unique=True)
except Exception:
    pass

# ── Config ──────────────────────────────────────────────────────────
CHUNK_SIZE = 50_000


# ── Helpers ─────────────────────────────────────────────────────────
def _parse_timestamp(ts_val: str) -> datetime:
    if not ts_val:
        raise ValueError("empty TimeStamp")
    s = str(ts_val).strip()
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.strptime(ts_val, "%Y-%m-%d %H:%M:%S")


def _to_float(val, field_name: str) -> float:
    if val is None or val == "":
        raise ValueError(f"{field_name} empty")
    return float(val)


def get_ist_datetime() -> datetime:
    """Return IST datetime"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


# ── Routes ─────────────────────────────────────────────────────────
@plantAPI.route("/bulk-add", methods=["POST"])
def bulk_add_plant_consumption():
    """
    Bulk Add Plant Consumption (staging)
    ---
    tags:
      - Plant
    parameters:
      - in: header
        name: X-User-Email
        type: string
        required: false
        description: Uploader email
      - in: body
        name: body
        required: true
        schema:
          type: array
          items:
            type: object
            properties:
              TimeStamp:
                type: string
                example: "2025-08-22 18:00:00"
              Plant_Name:
                type: string
                example: "WI"
              Actual:
                type: number
                example: 1655.42
              Pred:
                type: number
                example: 0
    responses:
      200:
        description: Bulk insert/update summary
    """
    try:
        data = request.get_json(silent=True, force=True)
        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400
        if not data:
            return jsonify({"message": "No records received"}), 200

        user_email = (request.headers.get("X-User-Email") or "").strip()

        ops, total_upserts, total_matched, total_modified = [], 0, 0, 0
        skipped_invalid, first_errors = 0, []

        def flush_ops():
            nonlocal ops, total_upserts, total_matched, total_modified
            if not ops:
                return
            result = collection.bulk_write(ops, ordered=False, bypass_document_validation=True)
            total_upserts += result.upserted_count or 0
            total_matched += result.matched_count or 0
            total_modified += result.modified_count or 0
            ops.clear()

        now_ist = get_ist_datetime()

        for i, item in enumerate(data):
            try:
                ts = _parse_timestamp(item.get("TimeStamp"))
                plant_name = (item.get("Plant_Name") or "").strip()
                if not plant_name:
                    raise ValueError("Plant_Name empty")
                actual = _to_float(item.get("Actual"), "Actual")
                pred_val = float(item.get("Pred")) if item.get("Pred") not in (None, "") else 0.0

                doc = {
                    "TimeStamp": ts,
                    "Plant_Name": plant_name,
                    "Actual": actual,
                    "Pred": pred_val,
                    "uploaded_by": user_email or None,
                    "uploaded_at": now_ist,
                }
                ops.append(ReplaceOne({"TimeStamp": ts, "Plant_Name": plant_name}, doc, upsert=True))

                if len(ops) >= CHUNK_SIZE:
                    flush_ops()
            except Exception as ex:
                skipped_invalid += 1
                if len(first_errors) < 5:
                    first_errors.append({"row_index": i, "error": str(ex), "row_sample": item})

        flush_ops()
        return jsonify({
            "message": "Bulk add completed",
            "received": len(data),
            "inserted_new": total_upserts,
            "replaced_existing": total_matched,
            "modified_existing": total_modified,
            "skipped_invalid": skipped_invalid,
            "chunk_size": CHUNK_SIZE,
            "sample_errors": first_errors,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===========================================================
# 🔷 Approval APIs
# ===========================================================
@plantAPI.route("/approvals", methods=["GET"])
def get_plant_approvals():
    """
    Get Plant Consumption Approvals
    ---
    tags:
      - Plant
    parameters:
      - in: query
        name: sort
        type: string
        required: false
        description: Field to sort by (default TimeStamp)
      - in: query
        name: order
        type: string
        enum: [asc, desc]
        required: false
        description: Sort order
      - in: query
        name: limit
        type: integer
        required: false
        description: Limit number of records
    responses:
      200:
        description: List of staged plant consumption records
    """
    try:
        sort_field = request.args.get("sort", "TimeStamp")
        order = request.args.get("order", "asc").lower()
        limit = int(request.args.get("limit", 100))
        sort_order = ASCENDING if order == "asc" else DESCENDING

        cursor = collection.find().sort(sort_field, sort_order).limit(limit)
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@plantAPI.route("/approvals/approve", methods=["POST"])
def approve_plant_data():
    """
    Approve Plant Consumption Data
    ---
    tags:
      - Plant
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            ids:
              type: array
              items:
                type: string
              example: ["6501b1fcd9f1e2a12c3b4567"]
    responses:
      200:
        description: Migration summary
      400:
        description: Invalid request
    """
    try:
        body = request.get_json(force=True)
        ids = body.get("ids", [])
        if not ids or not isinstance(ids, list):
            return jsonify({"error": "ids must be a non-empty list"}), 400

        object_ids = [ObjectId(i) for i in ids]
        docs = list(collection.find({"_id": {"$in": object_ids}}))
        if not docs:
            return jsonify({"error": "No matching documents found"}), 404

        ops = []
        for doc in docs:
            doc.pop("_id", None)
            ops.append(ReplaceOne({"TimeStamp": doc["TimeStamp"], "Plant_Name": doc["Plant_Name"]}, doc, upsert=True))

        if ops:
            result = final_collection.bulk_write(ops, ordered=False)
            collection.delete_many({"_id": {"$in": object_ids}})
            return jsonify({
                "message": "Plant approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
