# demandroute.py (Main Application Entry Point)
from flask import Flask, jsonify, request
from flask_cors import CORS

# from Routes.uploadRoutes import uploadApi
# from Routes.plantroute import plant_bp
from Routes.demandroute import demand_bp  # Import the demand blueprint

app = Flask(__name__)

# Enable CORS for all routes and origins.
CORS(app, resources={r"/*": {"origins": "*"}})

# Register blueprints.
# app.register_blueprint(uploadApi, url_prefix='/upload')
# app.register_blueprint(plant_bp, url_prefix='/plant')
app.register_blueprint(demand_bp,url_prefix="/demand")  # Registers the /demand route.

@app.route('/')
def hello_world():
    return 'GUVNL Alternative Server is running!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)