from flask import Blueprint, request, jsonify
from pymongo import MongoClient, ReplaceOne, ASCENDING, DESCENDING
from bson import ObjectId
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

bankingAPI = Blueprint("bankingAPI", __name__)

# Mongo setup
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["power_casting_new"]

approval_collection = db["Banking_Adjust_Consolidated_approval"]
final_collection = db["Banking_Adjust_Consolidated"]

# Ensure unique index on Timestamp
try:
    approval_collection.create_index([("Timestamp", ASCENDING)], unique=True)
except Exception:
    pass


# ===============================
# GET Approval Records
# ===============================
@bankingAPI.route("/approvals", methods=["GET"])
def get_banking_approvals():
    """
    Get Banking Adjustment Approval Records
    ---
    tags:
      - Banking
    parameters:
      - name: sort
        in: query
        type: string
        required: false
        description: "Field to sort by (default: Timestamp)"
      - name: order
        in: query
        type: string
        enum: [asc, desc]
        required: false
        description: "Sort direction (asc or desc)"
      - name: limit
        in: query
        type: integer
        required: false
        description: "Maximum number of records to return"
    responses:
      200:
        description: "List of approval records"
    """
    try:
        sort_field = request.args.get("sort", "Timestamp")
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


# ===============================
# POST Approve Records (migrate)
# ===============================
@bankingAPI.route("/approvals/approve", methods=["POST"])
def approve_banking_data():
    """
    Approve selected banking records and migrate to final collection
    ---
    tags:
      - Banking
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
              example: ["650d5e8a88cd5a12c343b123", "650d5e8a88cd5a12c343b124"]
    responses:
      200:
        description: "Migration summary"
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
            ts = doc.get("Timestamp")
            doc.pop("_id", None)
            ops.append(ReplaceOne({"Timestamp": ts}, doc, upsert=True))

        if ops:
            result = final_collection.bulk_write(ops, ordered=False)
            approval_collection.delete_many({"_id": {"$in": object_ids}})
            return jsonify({
                "message": "Banking approval migration completed",
                "migrated": len(docs),
                "inserted_new": result.upserted_count or 0,
                "updated_existing": result.modified_count or 0,
                "deleted_from_approval": len(docs)
            }), 200
        else:
            return jsonify({"message": "No operations executed"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===============================
# PATCH Edit Approval Record
# ===============================
@bankingAPI.route("/approvals/<approval_id>", methods=["PATCH"])
def edit_banking_approval(approval_id):
    """
    Edit a banking approval record
    ---
    tags:
      - Banking
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
        description: "ObjectId of the record"
      - in: body
        name: body
        required: true
        schema:
          type: object
          additionalProperties: true
    responses:
      200:
        description: "Update confirmation"
    """
    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict) or not data:
            return jsonify({"error": "Request must contain fields to update"}), 400

        result = approval_collection.update_one(
            {"_id": ObjectId(approval_id)},
            {"$set": data}
        )

        if result.matched_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({
            "message": "Approval record updated",
            "updated_fields": data
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===============================
# DELETE Approval Record
# ===============================
@bankingAPI.route("/approvals/<approval_id>", methods=["DELETE"])
def delete_banking_approval(approval_id):
    """
    Delete a banking approval record
    ---
    tags:
      - Banking
    parameters:
      - in: path
        name: approval_id
        type: string
        required: true
        description: "ObjectId of the record"
    responses:
      200:
        description: "Deletion confirmation"
    """
    try:
        result = approval_collection.delete_one({"_id": ObjectId(approval_id)})
        if result.deleted_count == 0:
            return jsonify({"error": "Approval record not found"}), 404

        return jsonify({"message": "Approval record deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
