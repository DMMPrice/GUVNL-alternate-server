# demandroute.py (Main Application Entry Point)
from flask import Flask, jsonify, request
from flask_cors import CORS

from Routes.ProcurementOutputRoutes import mongoDemandOutput_bp  # Import the demand blueprint
from Routes.DemandDataAdditionRoutes import demandAPI
from Routes.IEXDataAdditionRoutes import iexAPI
from Routes.PlantDataAddition import mongoPlant_bp

app = Flask(__name__)

# Enable CORS for all routes and origins.
CORS(app, resources={r"/*": {"origins": "*"}})

# Register blueprints.
app.register_blueprint(mongoDemandOutput_bp, url_prefix="/procurement-output")  # Registers the /demand route.
app.register_blueprint(demandAPI, url_prefix='/demand')  # Registers the /demand route
app.register_blueprint(iexAPI, url_prefix='/iex')
app.register_blueprint(mongoPlant_bp, url_prefix='/plant-consumption')  # Registers the /plant-consumption route



@app.route('/')
def hello_world():
    return 'GUVNL Alternative Server is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)
