# Routes/demandRoute.py
from flask import Blueprint, jsonify, request
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import json  # Import json module to dump JSON data as string

demand_bp = Blueprint('demand_bp', __name__)

# MySQL database connection configuration
db_config = {
    'user': 'DB-Admin',
    'password': 'DBTest@123',
    'host': '69.62.74.149',
    'database': 'guvnldev'
}

def parse_timestamp(ts_str):
    """
    Convert a timestamp string of format 'Sat, 01 Apr 2023 00:00:00 GMT'
    into a Python datetime object.
    """
    ts_str = ts_str.replace(" GMT", "")
    return datetime.strptime(ts_str, '%a, %d %b %Y %H:%M:%S')


@demand_bp.route('/', methods=['GET'])
def get_demand():
    return jsonify({"message": "GET endpoint on /demand is working"}), 200

@demand_bp.route('/', methods=['POST'])
def post_demand():
    data = request.get_json()

    # Get and convert the timestamp from the payload.
    ts_str = data.get('TimeStamp') or data.get('timestamp')
    if not ts_str:
        return jsonify({"error": "Timestamp is missing"}), 400
    try:
        parsed_timestamp = parse_timestamp(ts_str)
    except ValueError as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    # SQL query with ON DUPLICATE KEY UPDATE to replace old data with new data
    insert_query = """
    INSERT INTO demand_output (
        timestamp,
        banking_unit,
        cost_per_block,
        demand_actual,
        demand_pred,
        demand_banked,
        iex_cost,
        last_price,
        must_run_total_cost,
        must_run_total_gen,
        remaining_plants_total_cost,
        remaining_plants_total_gen,
        iex_data,
        backdown_total_cost,
        iex_gen,
        must_run,
        remaining_plants
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        banking_unit = VALUES(banking_unit),
        cost_per_block = VALUES(cost_per_block),
        demand_actual = VALUES(demand_actual),
        demand_pred = VALUES(demand_pred),
        demand_banked = VALUES(demand_banked),
        iex_cost = VALUES(iex_cost),
        last_price = VALUES(last_price),
        must_run_total_cost = VALUES(must_run_total_cost),
        must_run_total_gen = VALUES(must_run_total_gen),
        remaining_plants_total_cost = VALUES(remaining_plants_total_cost),
        remaining_plants_total_gen = VALUES(remaining_plants_total_gen),
        iex_data = VALUES(iex_data),
        backdown_total_cost = VALUES(backdown_total_cost),
        iex_gen = VALUES(iex_gen),
        must_run = VALUES(must_run),
        remaining_plants = VALUES(remaining_plants);
    """

    values = (
        parsed_timestamp,
        data.get("Banking_Unit"),
        data.get("Cost_Per_Block"),
        data.get("Demand(Actual)"),
        data.get("Demand(Pred)"),
        data.get("Demand_Banked"),
        data.get("IEX_Cost"),
        data.get("Last_Price"),
        data.get("Must_Run_Total_Cost"),
        data.get("Must_Run_Total_Gen"),
        data.get("Remaining_Plants_Total_Cost"),
        data.get("Remaining_Plants_Total_Gen"),
        json.dumps(data.get("IEX_Data")),
        data.get("Backdown_Cost"),
        data.get("IEX_Gen"),
        json.dumps(data.get("Must_Run")),
        json.dumps(data.get("Remaining_Plants"))
    )

    try:
        # Connect to MySQL database using the provided configuration.
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            cursor.close()
            conn.close()
            return jsonify({"message": "Data inserted/updated successfully"}), 201
        else:
            return jsonify({"error": "Failed to connect to the database"}), 500
    except Error as e:
        return jsonify({"error": str(e)}), 500
