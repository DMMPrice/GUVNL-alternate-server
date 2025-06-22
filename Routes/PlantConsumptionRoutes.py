# demandroute.py (Main Application Entry Point)
from flask import Flask, jsonify, request
from flask_cors import CORS

from Routes.ProcurementOutputRoutes import procurementOutput_bp  # Import the demand blueprint
from Routes.PlantConsumptionRoutes import plant_consumption_bp  # Import the plant consumption blueprint

app = Flask(__name__)

# Enable CORS for all routes and origins.
CORS(app, resources={r"/*": {"origins": "*"}})

# Register blueprints.
# app.register_blueprint(uploadApi, url_prefix='/upload')
# app.register_blueprint(plant_bp, url_prefix='/plant')
app.register_blueprint(procurementOutput_bp,url_prefix="/procurement-output")  # Registers the /demand route.
app.register_blueprint(plant_consumption_bp, url_prefix="/plant-consumption")  # Registers the /Powercasting route

@app.route('/')
def hello_world():
    return 'GUVNL Alternative Server is running!'

@app.route('/plant-consumption/Powercasting', methods=['GET'])
def powercasting():
    # Your logic for Powercasting route
    return jsonify({"message": "Welcome to the Powercasting route!"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)