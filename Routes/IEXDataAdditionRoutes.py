# iex_api.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ReplaceOne, ASCENDING, DESCENDING
from bson import ObjectId
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

iexAPI = Blueprint("iexAPI", __name__)

# --- MongoDB setup ---
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, maxPoolSize=200)
db = client["powercasting"]

# Approval collections
price_collection = db["IEX_Price_approval"]
gen_collection = db["IEX_Generation_approval"]

# Final collections
price_final = db["IEX_Price"]
gen_final = db["IEX_Generation"]

# Ensure unique index on TimeStamp for both staging collections
try:
    price_collection.create_index([("TimeStamp", ASCENDING)], unique=True)
    gen_collection.create_index([("TimeStamp", ASCENDING)], unique=True)
except Exception:
    pass

CHUNK_SIZE = 50_000


# --- Helpers ---
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


def get_ist_datetime() -> datetime:
    """Return IST datetime object (BSON Date compatible)"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)


# ===========================================================
# 🔷 Bulk Add Price Data
# ===========================================================
@iexAPI.route("/price/bulk-add", methods=["POST"])
def bulk_add_price_data():
    """
    Bulk Add IEX Price Data
    ---
    tags:
      - IEX
    parameters:
      - in: header
        name: X-User-Email
        type: string
        required: false
        description: Email of the uploader
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
              Actual:
                type: number
                example: 350.5
              Pred:
                type: number
                example: 355.0
    responses:
      200:
        description: Bulk insert/update summary
    """
    try:
        uploader = request.headers.get("X-User-Email", "").strip()
        data = request.get_json(silent=True, force=True)
        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400
        if not data:
            return jsonify({"message": "No records received"}), 200

        ops, total_upserts, total_matched, total_modified = [], 0, 0, 0
        skipped_invalid, first_errors = 0, []

        def flush_ops():
            nonlocal ops, total_upserts, total_matched, total_modified
            if not ops:
                return
            result = price_collection.bulk_write(
                ops, ordered=False, bypass_document_validation=True
            )
            total_upserts += result.upserted_count or 0
            total_matched += result.matched_count or 0
            total_modified += result.modified_count or 0
            ops = []

        for i, item in enumerate(data):
            try:
                ts = _parse_timestamp(item.get("TimeStamp"))
                actual = _to_float(item.get("Actual"), "Actual")

                pred_present = "Pred" in item and item.get("Pred") != ""
                pred = _to_float(item.get("Pred"), "Pred") if pred_present else None

                doc = {
                    "TimeStamp": ts,
                    "Actual": actual,
                    **({"Pred": pred} if pred_present else {}),
                    "uploaded_by": uploader or None,
                    "uploaded_at": get_ist_datetime(),
                }

                ops.append(ReplaceOne({"TimeStamp": ts}, doc, upsert=True))

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
# 🔷 Bulk Add Generation Data
# ===========================================================
@iexAPI.route("/quantity/bulk-add", methods=["POST"])
def bulk_add_iex_data():
    """
    Bulk Add IEX Quantity Data
    ---
    tags:
      - IEX
    parameters:
      - in: header
        name: X-User-Email
        type: string
        required: false
        description: Email of the uploader
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
              Qty_Pred:
                type: number
                example: 500.75
              Pred_Price:
                type: number
                example: 350.5
    responses:
      200:
        description: Bulk insert/update summary
    """
    try:
        uploader = request.headers.get("X-User-Email", "").strip()
        data = request.get_json(silent=True, force=True)
        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400
        if not data:
            return jsonify({"message": "No records received"}), 200

        ops, total_upserts, total_matched, total_modified = [], 0, 0, 0
        skipped_invalid, first_errors = 0, []

        def flush_ops():
            nonlocal ops, total_upserts, total_matched, total_modified
            if not ops:
                return
            result = gen_collection.bulk_write(
                ops, ordered=False, bypass_document_validation=True
            )
            total_upserts += result.upserted_count or 0
            total_matched += result.matched_count or 0
            total_modified += result.modified_count or 0
            ops = []

        for i, item in enumerate(data):
            try:
                ts = _parse_timestamp(item.get("TimeStamp"))
                qty = _to_float(item.get("Qty_Pred"), "Qty_Pred")
                price = _to_float(item.get("Pred_Price"), "Pred_Price")

                doc = {
                    "TimeStamp": ts,
                    "Qty_Pred": qty,
                    "Pred_Price": price,
                    "uploaded_by": uploader or None,
                    "uploaded_at": get_ist_datetime(),
                }

                ops.append(ReplaceOne({"TimeStamp": ts}, doc, upsert=True))

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
# ✅ Approval APIs for Price
# ===========================================================
@iexAPI.route("/price/approvals", methods=["GET"])
def get_price_approvals():
    """
    Get IEX Price Approvals
    ---
    tags:
      - IEX
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
        description: List of staged price records
    """
    try:
        sort_field = request.args.get("sort", "TimeStamp")
        order = request.args.get("order", "asc").lower()
        limit = int(request.args.get("limit", 100))
        sort_order = ASCENDING if order == "asc" else DESCENDING

        cursor = price_collection.find().sort(sort_field, sort_order).limit(limit)
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/price/approvals/approve", methods=["POST"])
def approve_price_data():
    """
    Approve IEX Price Data
    ---
    tags:
      - IEX
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
        docs = list(price_collection.find({"_id": {"$in": object_ids}}))
        if not docs:
            return jsonify({"error": "No matching documents found"}), 404

        ops = []
        for doc in docs:
            doc.pop("_id", None)
            ops.append(ReplaceOne({"TimeStamp": doc["TimeStamp"]}, doc, upsert=True))

        if ops:
            result = price_final.bulk_write(ops, ordered=False)
            price_collection.delete_many({"_id": {"$in": object_ids}})
            return jsonify({
                "message": "Price approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/price/approvals/<approval_id>", methods=["PATCH"])
def edit_price_approval(approval_id):
    """
    Edit a price approval record
    ---
    tags:
      - IEX
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            Actual:
              type: number
            Pred:
              type: number
    responses:
      200:
        description: Updated record
    """
    try:
        data = request.get_json(force=True)
        update_fields = {}

        if "Actual" in data:
            update_fields["Actual"] = _to_float(data["Actual"], "Actual")

        if "Pred" in data:
            update_fields["Pred"] = _to_float(data["Pred"], "Pred")

        if not update_fields:
            return jsonify({"error": "No valid fields provided"}), 400

        result = price_collection.update_one(
            {"_id": ObjectId(approval_id)},
            {"$set": update_fields}
        )

        if result.matched_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({"message": "Price approval record updated", "fields_updated": update_fields}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/price/approvals/<approval_id>", methods=["DELETE"])
def delete_price_approval(approval_id):
    """
    Delete a price approval record
    ---
    tags:
      - IEX
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
    responses:
      200:
        description: Record deleted
    """
    try:
        result = price_collection.delete_one({"_id": ObjectId(approval_id)})
        if result.deleted_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({"message": "Price approval record deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===========================================================
# ✅ Approval APIs for Generation
# ===========================================================
@iexAPI.route("/quantity/approvals", methods=["GET"])
def get_quantity_approvals():
    """
    Get IEX Quantity Approvals
    ---
    tags:
      - IEX
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
        description: List of staged quantity records
    """
    try:
        sort_field = request.args.get("sort", "TimeStamp")
        order = request.args.get("order", "asc").lower()
        limit = int(request.args.get("limit", 100))
        sort_order = ASCENDING if order == "asc" else DESCENDING

        cursor = gen_collection.find().sort(sort_field, sort_order).limit(limit)
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/quantity/approvals/approve", methods=["POST"])
def approve_quantity_data():
    """
    Approve IEX Quantity Data
    ---
    tags:
      - IEX
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
        docs = list(gen_collection.find({"_id": {"$in": object_ids}}))
        if not docs:
            return jsonify({"error": "No matching documents found"}), 404

        ops = []
        for doc in docs:
            doc.pop("_id", None)
            ops.append(ReplaceOne({"TimeStamp": doc["TimeStamp"]}, doc, upsert=True))

        if ops:
            result = gen_final.bulk_write(ops, ordered=False)
            gen_collection.delete_many({"_id": {"$in": object_ids}})
            return jsonify({
                "message": "Generation approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/quantity/approvals/<approval_id>", methods=["PATCH"])
def edit_quantity_approval(approval_id):
    """
    Edit a generation (quantity) approval record
    ---
    tags:
      - IEX
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            Qty_Pred:
              type: number
            Pred_Price:
              type: number
    responses:
      200:
        description: Updated record
    """
    try:
        data = request.get_json(force=True)
        update_fields = {}

        if "Qty_Pred" in data:
            update_fields["Qty_Pred"] = _to_float(data["Qty_Pred"], "Qty_Pred")

        if "Pred_Price" in data:
            update_fields["Pred_Price"] = _to_float(data["Pred_Price"], "Pred_Price")

        if not update_fields:
            return jsonify({"error": "No valid fields provided"}), 400

        result = gen_collection.update_one(
            {"_id": ObjectId(approval_id)},
            {"$set": update_fields}
        )

        if result.matched_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({"message": "Quantity approval record updated", "fields_updated": update_fields}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexAPI.route("/quantity/approvals/<approval_id>", methods=["DELETE"])
def delete_quantity_approval(approval_id):
    """
    Delete a generation (quantity) approval record
    ---
    tags:
      - IEX
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
    responses:
      200:
        description: Record deleted
    """
    try:
        result = gen_collection.delete_one({"_id": ObjectId(approval_id)})
        if result.deleted_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({"message": "Quantity approval record deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
