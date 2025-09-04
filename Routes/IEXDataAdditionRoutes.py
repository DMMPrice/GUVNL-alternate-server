# iex_api.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ReplaceOne, ASCENDING
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

iexAPI = Blueprint("iexAPI", __name__)

# --- MongoDB setup ---
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, maxPoolSize=200)
db = client["powercasting"]
price_collection = db["IEX_Price_Test"]
gen_collection = db["IEX_Generation_Test"]

# Ensure unique index on TimeStamp for both collections
try:
    price_collection.create_index([("TimeStamp", ASCENDING)], unique=True)
    gen_collection.create_index([("TimeStamp", ASCENDING)], unique=True)
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


# 🔷 Route 1: Bulk insert IEX Price data
@iexAPI.route("/price/bulk-add", methods=["POST"])
def bulk_add_price_data():
    try:
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
                pred = _to_float(item.get("Pred"), "Pred")

                doc = {"TimeStamp": ts, "Actual": actual, "Pred": pred}

                ops.append(ReplaceOne({"TimeStamp": ts}, doc, upsert=True))

                if len(ops) >= CHUNK_SIZE:
                    flush_ops()

            except Exception as ex:
                skipped_invalid += 1
                if len(first_errors) < 5:
                    first_errors.append(
                        {"row_index": i, "error": str(ex), "row_sample": item}
                    )

        flush_ops()

        return jsonify(
            {
                "message": "Bulk add completed",
                "received": len(data),
                "inserted_new": total_upserts,
                "replaced_existing": total_matched,
                "modified_existing": total_modified,
                "skipped_invalid": skipped_invalid,
                "chunk_size": CHUNK_SIZE,
                "sample_errors": first_errors,
            }
        ), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 🔷 Route 2: Bulk insert IEX Generation data
@iexAPI.route("/quantity/bulk-add", methods=["POST"])
def bulk_add_iex_data():
    try:
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

                doc = {"TimeStamp": ts, "Qty_Pred": qty, "Pred_Price": price}

                ops.append(ReplaceOne({"TimeStamp": ts}, doc, upsert=True))

                if len(ops) >= CHUNK_SIZE:
                    flush_ops()

            except Exception as ex:
                skipped_invalid += 1
                if len(first_errors) < 5:
                    first_errors.append(
                        {"row_index": i, "error": str(ex), "row_sample": item}
                    )

        flush_ops()

        return jsonify(
            {
                "message": "Bulk add completed",
                "received": len(data),
                "inserted_new": total_upserts,
                "replaced_existing": total_matched,
                "modified_existing": total_modified,
                "skipped_invalid": skipped_invalid,
                "chunk_size": CHUNK_SIZE,
                "sample_errors": first_errors,
            }
        ), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
