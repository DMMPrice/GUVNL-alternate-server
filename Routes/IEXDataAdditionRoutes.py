from flask import Blueprint, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

iexAPI = Blueprint('iexAPI', __name__)

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["powercasting"]
price_collection = db["IEX_Price"]
iex_collection = db["IEX_Generation"]


# 🔷 Route 1: Bulk insert IEX Price data
@iexAPI.route('/price/bulk-add', methods=['POST'])
def bulk_add_price_data():
    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400

        inserted = 0
        replaced = 0

        for item in data:
            if "TimeStamp" not in item or "Actual" not in item or "Pred" not in item:
                return jsonify({"error": "Each item must contain 'TimeStamp', 'Actual', and 'Pred'"}), 400

            ts = datetime.fromisoformat(item["TimeStamp"])
            actual = float(item["Actual"])
            pred = float(item["Pred"])

            result = price_collection.delete_one({"TimeStamp": ts})
            if result.deleted_count > 0:
                replaced += 1

            price_collection.insert_one({
                "TimeStamp": ts,
                "Actual": actual,
                "Pred": pred
            })
            inserted += 1

        return jsonify({"message": f"{inserted} inserted, {replaced} replaced"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 🔷 Route 2: Bulk insert IEX Quantity & Predicted Price data
@iexAPI.route('/quantity/bulk-add', methods=['POST'])
def bulk_add_iex_data():
    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({"error": "Payload must be a list of records"}), 400

        inserted = 0
        replaced = 0

        for item in data:
            if "TimeStamp" not in item or "Qty_Pred" not in item or "Pred_Price" not in item:
                return jsonify({"error": "Each item must contain 'TimeStamp', 'Qty_Pred', and 'Pred_Price'"}), 400

            ts = datetime.fromisoformat(item["TimeStamp"])
            qty = float(item["Qty_Pred"])
            price = float(item["Pred_Price"])

            result = iex_collection.delete_one({"TimeStamp": ts})
            if result.deleted_count > 0:
                replaced += 1

            iex_collection.insert_one({
                "TimeStamp": ts,
                "Qty_Pred": qty,
                "Pred_Price": price
            })
            inserted += 1

        return jsonify({"message": f"{inserted} inserted, {replaced} replaced"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
