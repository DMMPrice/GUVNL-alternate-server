from flask import Blueprint, request, jsonify
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

mongoPlant_bp = Blueprint("mongoPlant_bp", __name__)

# ─── MongoDB connection ─────────────────────────────────────────────
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["powercasting"]
plant_coll = db["mustrunplantconsumption"]          # 👈 collection name

# ─── Helpers ────────────────────────────────────────────────────────
def parse_ts(ts_str: str) -> datetime:
    """
    Accepts either:
      • ISO-8601 (e.g. 2021-01-04T00:00:00.000+00:00 / 2021-01-04T00:00:00Z)
      • 'Sat, 01 Apr 2023 00:00:00 GMT'
    """
    ts_str = ts_str.strip()
    try:
        # ISO–8601 branch
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except ValueError:
        # RFC-1123 branch
        return datetime.strptime(ts_str.replace(" GMT", ""), "%a, %d %b %Y %H:%M:%S")

def build_doc(raw):
    """Keep only the structure we need."""
    ts = parse_ts(raw.get("TimeStamp") or raw.get("timestamp", ""))
    doc = {
        "TimeStamp": ts,
        "Plant_Name": raw.get("Plant_Name", "").strip(),
    }
    if "Actual" in raw:
        doc["Actual"] = float(raw["Actual"])
    if "Pred" in raw:
        doc["Pred"] = float(raw["Pred"])
    return doc

# ─── Routes ─────────────────────────────────────────────────────────
@mongoPlant_bp.route("/", methods=["POST"])
def add_plant_data():
    """
    POST /plant-consumption
    Accepts EITHER a single JSON object OR a list of objects:

    {
      "TimeStamp": "2021-01-04T00:00:00.000Z",
      "Actual": 1655.42,
      "Pred":   1523.74,
      "Plant_Name": "WI"
    }
    """
    payload = request.get_json()
    incoming = payload if isinstance(payload, list) else [payload]

    docs_to_insert = []
    for item in incoming:
        try:
            doc = build_doc(item)
        except (ValueError, KeyError) as e:
            return jsonify({"error": f"Bad record format – {e}"}), 400

        # Skip if this exact (TimeStamp, Plant_Name) already exists
        if plant_coll.find_one({"TimeStamp": doc["TimeStamp"],
                                "Plant_Name": doc["Plant_Name"]}):
            continue
        docs_to_insert.append(doc)

    if not docs_to_insert:
        return jsonify({"message": "No new documents to insert"}), 200

    plant_coll.insert_many(docs_to_insert)
    return jsonify({"message": f"{len(docs_to_insert)} record(s) inserted"}), 201

