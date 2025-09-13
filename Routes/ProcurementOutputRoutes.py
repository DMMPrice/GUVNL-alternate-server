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
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)

db = client["powercasting"]
collection = db["Demand_Output"]                 # final table
approval_collection = db["Demand_Output_Approval"]  # staging table

# fields we store/update
OPTIONAL_FIELDS = [
    "Backdown_Cost", "Backdown_Cost_Min", "Backdown_Unit", "Banking_Unit",
    "Cost_Per_Block", "Demand(Actual)", "Demand(Pred)", "Demand_Banked",
    "IEX_Cost", "Last_Price", "Must_Run_Total_Cost", "Must_Run_Total_Gen",
    "Remaining_Plants_Total_Cost", "Remaining_Plants_Total_Gen",
    "IEX_Data", "IEX_Gen", "Must_Run", "Remaining_Plants"
]


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


# =============== CRUD APIs (Staging Save) ==================
@mongoDemandOutput_bp.route('/', methods=['POST'])
def post_demand_output():
    """
    Insert Demand Output (staging)
    ---
    tags:
      - Demand Output
    """
    status_code, response = 500, {}
    try:
        data = request.get_json()
        ts_str = data.get('TimeStamp') or data.get('timestamp')
        if not ts_str:
            response, status_code = {"error": "Timestamp is missing"}, 400
            return jsonify(response), status_code

        parsed_timestamp = parse_timestamp(ts_str)

        if approval_collection.find_one({"TimeStamp": parsed_timestamp}):
            response, status_code = {"message": "Document with this TimeStamp already exists"}, 200
            return jsonify(response), status_code

        user_email = request.headers.get("X-User-Email") or None
        doc = {"TimeStamp": parsed_timestamp, "author": user_email}

        for field in OPTIONAL_FIELDS:
            if field in data:
                doc[field] = data[field]

        approval_collection.insert_one(doc)
        response, status_code = {"message": "Document inserted successfully"}, 201
        return jsonify(response), status_code

    except Exception as e:
        response, status_code = {"error": str(e)}, 500
        return jsonify(response), status_code
    finally:
        log_transaction("/procurement-output/", "POST",
                        request.get_json(silent=True, force=True),
                        request.headers, status_code, response)


@mongoDemandOutput_bp.route('/', methods=['PUT'])
def put_demand_output():
    """
    Update Demand Output (staging)
    ---
    tags:
      - Demand Output
    """
    status_code, response = 500, {}
    try:
        data = request.get_json()
        ts_str = data.get('TimeStamp') or data.get('timestamp')
        if not ts_str:
            response, status_code = {"error": "Timestamp is required for update"}, 400
            return jsonify(response), status_code

        parsed_timestamp = parse_timestamp(ts_str)
        update_fields = {f: data[f] for f in OPTIONAL_FIELDS if f in data}
        if not update_fields:
            response, status_code = {"error": "No updatable fields provided"}, 400
            return jsonify(response), status_code

        update_fields["author"] = request.headers.get("X-User-Email") or None

        result = approval_collection.update_one({"TimeStamp": parsed_timestamp}, {"$set": update_fields})

        if result.matched_count:
            response, status_code = {"message": "Document updated successfully", "modified_count": result.modified_count}, 200
        else:
            response, status_code = {"error": "No document found for the given TimeStamp"}, 404

        return jsonify(response), status_code
    except Exception as e:
        response, status_code = {"error": str(e)}, 500
        return jsonify(response), status_code
    finally:
        log_transaction("/procurement-output/", "PUT",
                        request.get_json(silent=True, force=True),
                        request.headers, status_code, response)


@mongoDemandOutput_bp.route('/', methods=['DELETE'])
def delete_demand_output():
    """
    Delete Demand Output (staging)
    ---
    tags:
      - Demand Output
    """
    status_code, response = 500, {}
    try:
        ts_str = request.args.get('TimeStamp') or request.args.get('timestamp')
        if not ts_str:
            response, status_code = {"error": "Timestamp query parameter is required"}, 400
            return jsonify(response), status_code

        parsed_timestamp = parse_timestamp(ts_str)
        result = approval_collection.delete_one({"TimeStamp": parsed_timestamp})

        user_email = request.headers.get("X-User-Email") or None

        if result.deleted_count:
            response, status_code = {"message": "Document deleted successfully", "deleted_by": user_email}, 200
        else:
            response, status_code = {"error": "No document found for the given TimeStamp"}, 404

        return jsonify(response), status_code
    except Exception as e:
        response, status_code = {"error": str(e)}, 500
        return jsonify(response), status_code
    finally:
        log_transaction("/procurement-output/", "DELETE",
                        None, request.headers, status_code, response)


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