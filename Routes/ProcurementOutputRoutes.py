from flask import Blueprint, request, jsonify
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

mongoDemandOutput_bp = Blueprint('mongoDemandOutput_bp', __name__)

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["powercasting"]
collection = db["Demand_Output"]

# fields we store/update
OPTIONAL_FIELDS = [
    "Backdown_Cost", "Backdown_Cost_Min", "Backdown_Unit", "Banking_Unit",
    "Cost_Per_Block", "Demand(Actual)", "Demand(Pred)", "Demand_Banked",
    "IEX_Cost", "Last_Price", "Must_Run_Total_Cost", "Must_Run_Total_Gen",
    "Remaining_Plants_Total_Cost", "Remaining_Plants_Total_Gen",
    "IEX_Data", "IEX_Gen", "Must_Run", "Remaining_Plants"
]


def parse_timestamp(ts_str):
    """
    Try multiple formats:
      - 'Sat, 01 Apr 2023 00:00:00 GMT'
      - '2023-04-01 00:00'
    """
    s = ts_str.replace(" GMT", "").strip()
    for fmt in (
            '%a, %d %b %Y %H:%M:%S',  # original GMT format
            '%Y-%m-%d %H:%M'  # new YYYY-MM-DD HH:MM format
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {ts_str}")


@mongoDemandOutput_bp.route('/', methods=['POST'])
def post_demand_output():
    data = request.get_json()
    ts_str = data.get('TimeStamp') or data.get('timestamp')
    if not ts_str:
        return jsonify({"error": "Timestamp is missing"}), 400

    try:
        parsed_timestamp = parse_timestamp(ts_str)
    except ValueError as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    if collection.find_one({"TimeStamp": parsed_timestamp}):
        return jsonify({"message": "Document with this TimeStamp already exists"}), 200

    doc = {"TimeStamp": parsed_timestamp}
    for field in OPTIONAL_FIELDS:
        if field in data:
            doc[field] = data[field]

    try:
        collection.insert_one(doc)
        return jsonify({"message": "Document inserted successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mongoDemandOutput_bp.route('/', methods=['PUT'])
def put_demand_output():
    data = request.get_json()
    ts_str = data.get('TimeStamp') or data.get('timestamp')
    if not ts_str:
        return jsonify({"error": "Timestamp is required for update"}), 400

    try:
        parsed_timestamp = parse_timestamp(ts_str)
    except ValueError as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    update_fields = {}
    for field in OPTIONAL_FIELDS:
        if field in data:
            update_fields[field] = data[field]

    if not update_fields:
        return jsonify({"error": "No updatable fields provided"}), 400

    result = collection.update_one(
        {"TimeStamp": parsed_timestamp},
        {"$set": update_fields}
    )

    if result.matched_count:
        return jsonify({
            "message": "Document updated successfully",
            "modified_count": result.modified_count
        }), 200
    else:
        return jsonify({"error": "No document found for the given TimeStamp"}), 404


@mongoDemandOutput_bp.route('/', methods=['DELETE'])
def delete_demand_output():
    # Accept timestamp in query string
    ts_str = request.args.get('TimeStamp') or request.args.get('timestamp')
    if not ts_str:
        return jsonify({"error": "Timestamp query parameter is required"}), 400

    try:
        parsed_timestamp = parse_timestamp(ts_str)
    except ValueError as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    result = collection.delete_one({"TimeStamp": parsed_timestamp})
    if result.deleted_count:
        return jsonify({"message": "Document deleted successfully"}), 200
    else:
        return jsonify({"error": "No document found for the given TimeStamp"}), 404
