from flask import Blueprint, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

demandAPI = Blueprint('demandAPI', __name__)

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["powercasting"]
collection = db["Demand"]  # This is your time-series collection


@demandAPI.route('/bulk-add', methods=['POST'])
def bulk_add_demand_data():
    try:
        data = request.get_json()

        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400

        inserted = 0
        replaced = 0

        for item in data:
            if "TimeStamp" not in item or "Demand(Actual)" not in item or "Demand(Pred)" not in item:
                return jsonify({
                    "error": "Each item must contain 'TimeStamp', 'Demand(Actual)', and 'Demand(Pred)'"
                }), 400

            try:
                timestamp = datetime.fromisoformat(item["TimeStamp"])
                actual = float(item["Demand(Actual)"])
                predicted = float(item["Demand(Pred)"])
            except Exception as e:
                return jsonify({"error": f"Invalid data format: {str(e)}"}), 400

            doc = {
                "TimeStamp": timestamp,
                "Demand(Actual)": actual,
                "Demand(Pred)": predicted
            }

            # Delete existing record with same TimeStamp
            result = collection.delete_one({"TimeStamp": timestamp})
            if result.deleted_count > 0:
                replaced += 1

            # Insert the new/updated document
            collection.insert_one(doc)
            inserted += 1

        return jsonify({
            "message": f"{inserted} total inserted, {replaced} were replacements"
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
