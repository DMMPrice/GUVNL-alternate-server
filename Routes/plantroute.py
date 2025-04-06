from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import random

# Create a Blueprint for plant routes.
plant_bp = Blueprint('plant_bp', __name__)

@plant_bp.route('/plant/', methods=['GET'])
def plant_data():
    # Retrieve query parameters.
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    price_cap_str = request.args.get('price_cap', '10')  # default cap is 10 if not provided

    # Check if required parameters are provided.
    if not start_date_str or not end_date_str:
        return jsonify({"error": "start_date and end_date parameters are required."}), 400

    try:
        # Convert the parameters to the appropriate types.
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
        price_cap = float(price_cap_str)
    except Exception as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400

    # Generate sample data at 15-minute intervals between the start and end dates.
    data = []
    current_time = start_date
    while current_time <= end_date:
        sample_price = round(random.uniform(0, price_cap), 2)
        data.append({
            "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "price": sample_price,
            "price_cap": price_cap
        })
        current_time += timedelta(minutes=15)

    return jsonify({"data": data})