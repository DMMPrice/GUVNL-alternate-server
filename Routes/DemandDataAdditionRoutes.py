from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ReplaceOne, ASCENDING, DESCENDING
from bson import ObjectId
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

demandAPI = Blueprint("demandAPI", __name__)

# --- MongoDB setup ---
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, maxPoolSize=200)
db = client["powercasting"]

approval_collection = db["Demand_approval"]
main_collection = db["Demand"]

# Ensure a unique index on TimeStamp in approval table
try:
    approval_collection.create_index([("TimeStamp", ASCENDING)], unique=True)
except Exception:
    pass

# --- Config ---
CHUNK_SIZE = 50_000


def _parse_timestamp(ts_val: str) -> datetime:
    if not ts_val:
        raise ValueError("empty TimeStamp")
    try:
        return datetime.strptime(ts_val, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.fromisoformat(ts_val)


def _to_float(val, field_name: str) -> float:
    if val is None or val == "":
        raise ValueError(f"{field_name} empty")
    return float(val)


def get_ist_datetime():
    """Return IST datetime object (not string)"""
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now


# ===========================================================
# ✅ Existing: Bulk Add Demand Data
# ===========================================================
@demandAPI.route("/bulk-add", methods=["POST"])
def bulk_add_demand_data():
    """
    Bulk Add Demand Data
    ---
    tags:
      - Demand
    consumes:
      - application/json
    parameters:
      - in: header
        name: X-User-Email
        type: string
        required: false
        description: Email of uploader
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
              Demand(Actual):
                type: number
                example: 123.4
              Demand(Pred):
                type: number
                example: 125.0
    responses:
      200:
        description: Bulk insert/update summary
    """
    try:
        data = request.get_json(silent=True, force=True)

        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400

        total_received = len(data)
        if total_received == 0:
            return jsonify({"message": "No records received"}), 200

        user_email = (request.headers.get("X-User-Email") or "").strip()
        now_utc = get_ist_datetime()

        ops: list[ReplaceOne] = []
        total_upserts = 0
        total_matched = 0
        total_modified = 0
        skipped_invalid = 0
        first_errors = []

        def flush_ops():
            nonlocal ops, total_upserts, total_matched, total_modified
            if not ops:
                return
            result = approval_collection.bulk_write(
                ops,
                ordered=False,
                bypass_document_validation=True,
            )
            total_upserts += result.upserted_count or 0
            total_matched += result.matched_count or 0
            total_modified += result.modified_count or 0
            ops = []

        for i, item in enumerate(data):
            try:
                ts_raw = item.get("TimeStamp")
                act_raw = item.get("Demand(Actual)")

                ts = _parse_timestamp(ts_raw)
                actual = _to_float(act_raw, "Demand(Actual)")

                pred_present = "Demand(Pred)" in item and item.get("Demand(Pred)") != ""
                predicted = _to_float(item.get("Demand(Pred)"), "Demand(Pred)") if pred_present else None

                doc = {
                    "TimeStamp": ts,
                    "Demand(Actual)": actual,
                    "uploaded_by": user_email or None,
                    "uploaded_at": now_utc,
                }
                if pred_present:
                    doc["Demand(Pred)"] = predicted

                ops.append(
                    ReplaceOne(
                        {"TimeStamp": ts},
                        doc,
                        upsert=True,
                    )
                )

                if len(ops) >= CHUNK_SIZE:
                    flush_ops()

            except Exception as ex:
                skipped_invalid += 1
                if len(first_errors) < 5:
                    first_errors.append(
                        {
                            "row_index": i,
                            "error": str(ex),
                            "row_sample": {k: item.get(k) for k in ("TimeStamp", "Demand(Actual)", "Demand(Pred)")},
                        }
                    )
                continue

        flush_ops()

        summary = {
            "message": "Bulk add completed",
            "received": total_received,
            "inserted_new": total_upserts,
            "replaced_existing": total_matched,
            "modified_existing": total_modified,
            "skipped_invalid": skipped_invalid,
            "chunk_size": CHUNK_SIZE,
        }
        if first_errors:
            summary["sample_errors"] = first_errors

        return jsonify(summary), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===========================================================
# ✅ New: Get Demand Approvals
# ===========================================================
@demandAPI.route("/approvals", methods=["GET"])
def get_demand_approvals():
    """
    Get all demand approval records
    ---
    tags:
      - Demand
    parameters:
      - in: query
        name: sort
        type: string
        required: false
        description: Field to sort by (default TimeStamp)
        example: TimeStamp
      - in: query
        name: order
        type: string
        required: false
        description: asc or desc (default asc)
        example: desc
      - in: query
        name: limit
        type: integer
        required: false
        description: Number of records to fetch (default 100)
        example: 50
    responses:
      200:
        description: List of demand approval records
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


# ===========================================================
# ✅ New: Approve Demand Data
# ===========================================================
@demandAPI.route("/approvals/approve", methods=["POST"])
def approve_demand_data():
    """
    Approve demand data → move from Demand_approval to Demand
    ---
    tags:
      - Demand
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
              example: ["685e4144634cb7dfca945468", "685e4144634cb7dfca945469"]
    responses:
      200:
        description: Migration summary
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
            doc.pop("_id", None)  # remove old id
            ops.append(
                ReplaceOne({"TimeStamp": doc["TimeStamp"]}, doc, upsert=True)
            )

        if ops:
            result = main_collection.bulk_write(ops, ordered=False)
            approval_collection.delete_many({"_id": {"$in": object_ids}})

            return jsonify({
                "message": "Approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
