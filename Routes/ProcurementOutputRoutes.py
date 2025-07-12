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


def parse_timestamp(ts_str):
    """Convert 'Sat, 01 Apr 2023 00:00:00 GMT' to datetime object."""
    ts_str = ts_str.replace(" GMT", "")
    return datetime.strptime(ts_str, '%a, %d %b %Y %H:%M:%S')


@mongoDemandOutput_bp.route('/debug', methods=['GET'])
def debug_view():
    docs = list(collection.find({}, {"_id": 0}).limit(10))
    return jsonify(docs), 200


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

    # Check if a document already exists for this timestamp
    if collection.find_one({"TimeStamp": parsed_timestamp}):
        return jsonify({"message": "Document with this TimeStamp already exists"}), 200

    doc = {"TimeStamp": parsed_timestamp}

    # Check and include only available fields
    optional_fields = [
        "Backdown_Cost", "Backdown_Cost_Min", "Backdown_Unit", "Banking_Unit", "Cost_Per_Block", "Demand(Actual)",
        "Demand(Pred)", "Demand_Banked",
        "IEX_Cost", "Last_Price", "Must_Run_Total_Cost", "Must_Run_Total_Gen",
        "Remaining_Plants_Total_Cost", "Remaining_Plants_Total_Gen", "IEX_Data",
        "Backdown_Cost", "IEX_Gen", "Must_Run", "Remaining_Plants"
    ]

    for field in optional_fields:
        if field in data:
            doc[field] = data[field]

    try:
        collection.insert_one(doc)
        return jsonify({"message": "Document inserted successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500
