# demand_api.py
from flask import Blueprint, request, jsonify
from pymongo import MongoClient, UpdateOne, ReplaceOne, ASCENDING
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

demandAPI = Blueprint("demandAPI", __name__)

# --- MongoDB setup ---
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, maxPoolSize=200)  # larger pool helps with bursts
db = client["powercasting"]
collection = db["Demand_test"]

# Ensure a unique index on TimeStamp (do this once at startup)
# Unique index guarantees "one doc per TimeStamp" and makes upserts fast.
try:
    collection.create_index([("TimeStamp", ASCENDING)], unique=True)
except Exception:
    # Index may already exist or another process is creating it; safe to ignore
    pass

# --- Config ---
CHUNK_SIZE = 50_000  # tune for your infra (10k..100k is typical)


def _parse_timestamp(ts_val: str) -> datetime:
    """
    Fast path: strict 'YYYY-MM-DD HH:mm:ss'.
    Fallback: datetime.fromisoformat for safety (handles 'YYYY-MM-DD HH:mm:ss' too).
    Raises ValueError if invalid.
    """
    if not ts_val:
        raise ValueError("empty TimeStamp")

    # Try strict format first (fast & predictable)
    try:
        return datetime.strptime(ts_val, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Fallback: fromisoformat (accepts 'YYYY-MM-DD HH:mm[:ss[.ffffff]]')
        return datetime.fromisoformat(ts_val)


def _to_float(val, field_name: str) -> float:
    if val is None or val == "":
        raise ValueError(f"{field_name} empty")
    return float(val)


@demandAPI.route("/bulk-add", methods=["POST"])
def bulk_add_demand_data():
    """
    Accepts a JSON array of records:
    [
      {"TimeStamp": "2025-08-22 18:00:00", "Demand(Actual)": 123.4},
      ...
    ]
    - "Demand(Pred)" is optional; if present, it’s stored.
    - For same TimeStamp, document is replaced (upsert).
    - Processes in chunks with bulk_write for high throughput.
    """
    try:
        data = request.get_json(silent=True, force=True)

        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400

        total_received = len(data)
        if total_received == 0:
            return jsonify({"message": "No records received"}), 200

        ops: list[ReplaceOne] = []
        total_upserts = 0
        total_matched = 0
        total_modified = 0
        skipped_invalid = 0
        first_errors = []  # collect a few sample errors for debugging

        def flush_ops():
            nonlocal ops, total_upserts, total_matched, total_modified
            if not ops:
                return
            result = collection.bulk_write(
                ops,
                ordered=False,
                bypass_document_validation=True,  # skip schema checks for speed
            )
            total_upserts += result.upserted_count or 0
            total_matched += result.matched_count or 0
            total_modified += result.modified_count or 0
            ops = []

        for i, item in enumerate(data):
            try:
                # Required fields
                ts_raw = item.get("TimeStamp")
                act_raw = item.get("Demand(Actual)")

                ts = _parse_timestamp(ts_raw)
                actual = _to_float(act_raw, "Demand(Actual)")

                # Optional predicted field (frontend may omit)
                pred_present = "Demand(Pred)" in item and item.get("Demand(Pred)") != ""
                if pred_present:
                    predicted = _to_float(item.get("Demand(Pred)"), "Demand(Pred)")
                else:
                    predicted = None

                # Build clean document
                doc = {
                    "TimeStamp": ts,
                    "Demand(Actual)": actual,
                }
                if pred_present:
                    doc["Demand(Pred)"] = predicted

                # Replace by timestamp (upsert=True ensures insert-or-replace)
                ops.append(
                    ReplaceOne(
                        {"TimeStamp": ts},
                        doc,
                        upsert=True,
                    )
                )

                # Chunked flush
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

        # Flush any remaining operations
        flush_ops()

        # inserted_new = upserts
        # replaced_existing = matched_count (docs found & replaced; may include "no-op" replacements if identical)
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